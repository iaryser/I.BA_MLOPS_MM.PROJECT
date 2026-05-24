from pathlib import Path

import pandas as pd
import pytest

from inference.online_feature_loader import OnlineFeatureLoader


def make_online_features() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                    "2026-01-01 02:00:00+00:00",
                    "2026-01-01 03:00:00+00:00",
                    "2026-01-01 04:00:00+00:00",
                    "2026-01-01 05:00:00+00:00",
                ]
            ),
            "coin_id": [
                "bitcoin",
                "ethereum",
                "solana",
                "cardano",
                "dogecoin",
                "polkadot",
            ],
            "price": [100.0, 200.0, 50.0, 10.0, 1.0, 8.0],
            "market_cap": [
                1_000_000.0,
                2_000_000.0,
                500_000.0,
                100_000.0,
                50_000.0,
                80_000.0,
            ],
            "volume": [
                50_000.0,
                90_000.0,
                70_000.0,
                30_000.0,
                20_000.0,
                40_000.0,
            ],
            "return": [0.01, 0.02, -0.01, 0.03, -0.02, 0.01],
            "return_6": [0.05, 0.04, 0.03, 0.02, 0.01, -0.01],
            "volatility_24": [0.10, 0.20, 0.15, 0.12, 0.08, 0.11],
        }
    )


def write_online_features(tmp_path: Path, df: pd.DataFrame | None = None) -> Path:
    data_path = tmp_path / "online_features.parquet"

    if df is None:
        df = make_online_features()

    df.to_parquet(data_path, index=False)

    return data_path


def test_loader_reads_online_features_from_parquet(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    assert len(loader.df) == 6
    assert set(loader.df["coin_id"]) == {
        "bitcoin",
        "ethereum",
        "solana",
        "cardano",
        "dogecoin",
        "polkadot",
    }


def test_get_available_coins_returns_sorted_coin_ids(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.get_available_coins()

    assert result == [
        "bitcoin",
        "cardano",
        "dogecoin",
        "ethereum",
        "polkadot",
        "solana",
    ]


def test_load_features_returns_only_model_feature_columns(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_features("bitcoin")

    assert list(result.columns) == [
        "return",
        "return_6",
        "volatility_24",
    ]


def test_load_features_returns_features_for_requested_coin_only(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_features("ethereum")

    assert len(result) == 1
    assert result.iloc[0]["return"] == pytest.approx(0.02)
    assert result.iloc[0]["return_6"] == pytest.approx(0.04)
    assert result.iloc[0]["volatility_24"] == pytest.approx(0.20)


def test_load_features_returns_empty_dataframe_for_unknown_coin(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_features("unknown-coin")

    assert result.empty
    assert list(result.columns) == [
        "return",
        "return_6",
        "volatility_24",
    ]


def test_load_context_returns_full_row_as_dict(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_context("bitcoin")

    assert result["coin_id"] == "bitcoin"
    assert result["price"] == 100.0
    assert result["market_cap"] == 1_000_000.0
    assert result["volume"] == 50_000.0
    assert result["return"] == 0.01
    assert result["return_6"] == 0.05
    assert result["volatility_24"] == 0.10


def test_load_context_raises_index_error_for_unknown_coin(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    with pytest.raises(IndexError):
        loader.load_context("unknown-coin")


def test_load_top5_coins_returns_five_highest_volume_coins(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_top5_coins()

    assert [coin.coin_id for coin in result] == [
        "ethereum",
        "solana",
        "bitcoin",
        "polkadot",
        "cardano",
    ]
    assert [coin.volume for coin in result] == [
        90_000.0,
        70_000.0,
        50_000.0,
        40_000.0,
        30_000.0,
    ]


def test_load_top5_coins_returns_topcoin_schema_objects(tmp_path: Path) -> None:
    data_path = write_online_features(tmp_path)

    loader = OnlineFeatureLoader(data_path=data_path)

    result = loader.load_top5_coins()

    assert result[0].coin_id == "ethereum"
    assert isinstance(result[0].volume, float)


def test_reload_refreshes_dataframe_from_parquet(tmp_path: Path) -> None:
    initial_df = make_online_features()
    data_path = write_online_features(tmp_path, initial_df)

    loader = OnlineFeatureLoader(data_path=data_path)

    updated_df = initial_df.copy()
    updated_df.loc[updated_df["coin_id"] == "bitcoin", "price"] = 999.0
    updated_df.to_parquet(data_path, index=False)

    loader.reload()

    bitcoin_row = loader.df[loader.df["coin_id"] == "bitcoin"].iloc[0]

    assert bitcoin_row["price"] == 999.0
