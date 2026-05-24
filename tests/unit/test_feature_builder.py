import numpy as np
import pandas as pd
import pytest

from feature_pipeline.transforms.feature_builder import FeatureBuilder


def make_market_data(
    coin_id: str = "bitcoin",
    periods: int = 60,
    price_start: float = 100.0,
    price_step: float = 1.0,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "coin_id": [coin_id] * periods,
            "timestamp": pd.date_range(
                "2026-01-01 00:00:00",
                periods=periods,
                freq="h",
                tz="UTC",
            ),
            "price": [price_start + i * price_step for i in range(periods)],
            "market_cap": [1_000_000.0 + i * 1_000.0 for i in range(periods)],
            "volume": [50_000.0 + i * 100.0 for i in range(periods)],
        }
    )


def test_feature_builder_adds_expected_feature_columns() -> None:
    df = make_market_data(periods=60)

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    expected_columns = {
        "coin_id",
        "timestamp",
        "price",
        "market_cap",
        "volume",
        "return",
        "return_6",
        "return_12",
        "return_24",
        "ma_deviation_6",
        "ma_deviation_12",
        "ma_deviation_24",
        "volatility_6",
        "volatility_12",
        "volatility_24",
        "normalized_momentum_6",
        "normalized_momentum_12",
        "normalized_momentum_24",
        "log_volume_change_6",
        "log_volume_change_12",
        "log_volume_change_24",
        "volume_to_mcap",
        "target",
    }

    assert expected_columns.issubset(result.columns)


def test_feature_builder_drops_intermediate_helper_columns() -> None:
    df = make_market_data(periods=60)

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    assert "log_volume" not in result.columns
    assert "is_missing_raw_data" not in result.columns
    assert "future_price" not in result.columns


def test_feature_builder_output_has_no_nan_or_infinite_values() -> None:
    df = make_market_data(periods=60)

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    numeric_df = result.select_dtypes(include="number")

    assert not result.empty
    assert not numeric_df.isna().any().any()
    assert np.isfinite(numeric_df.to_numpy()).all()


def test_return_features_are_computed_per_coin() -> None:
    df = pd.concat(
        [
            make_market_data(
                coin_id="bitcoin",
                periods=60,
                price_start=100.0,
                price_step=1.0,
            ),
            make_market_data(
                coin_id="ethereum",
                periods=60,
                price_start=200.0,
                price_step=2.0,
            ),
        ],
        ignore_index=True,
    )

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    btc_first_row = result[result["coin_id"] == "bitcoin"].iloc[0]
    eth_first_row = result[result["coin_id"] == "ethereum"].iloc[0]

    assert btc_first_row["return"] == pytest.approx(
        btc_first_row["price"] / (btc_first_row["price"] - 1.0) - 1
    )
    assert eth_first_row["return"] == pytest.approx(
        eth_first_row["price"] / (eth_first_row["price"] - 2.0) - 1
    )


def test_target_is_one_when_future_price_is_higher() -> None:
    df = make_market_data(
        periods=60,
        price_start=100.0,
        price_step=1.0,
    )

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    assert set(result["target"]) == {1}


def test_target_is_zero_when_future_price_is_lower() -> None:
    df = make_market_data(
        periods=60,
        price_start=200.0,
        price_step=-1.0,
    )

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    assert set(result["target"]) == {0}


def test_target_does_not_leak_across_coins() -> None:
    bitcoin = make_market_data(
        coin_id="bitcoin",
        periods=60,
        price_start=100.0,
        price_step=1.0,
    )
    ethereum = make_market_data(
        coin_id="ethereum",
        periods=60,
        price_start=200.0,
        price_step=-1.0,
    )

    df = pd.concat([bitcoin, ethereum], ignore_index=True)

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    btc_targets = set(result[result["coin_id"] == "bitcoin"]["target"])
    eth_targets = set(result[result["coin_id"] == "ethereum"]["target"])

    assert btc_targets == {1}
    assert eth_targets == {0}


def test_include_target_false_does_not_create_target_column() -> None:
    df = make_market_data(periods=60)

    builder = FeatureBuilder(future_horizon=24, include_target=False)
    result = builder.build(df)

    assert "target" not in result.columns


def test_builder_sorts_by_coin_and_timestamp() -> None:
    df = make_market_data(periods=60)
    shuffled = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(shuffled)

    expected = result.sort_values(["coin_id", "timestamp"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)


def test_market_cap_must_be_positive() -> None:
    df = make_market_data(periods=60)
    df.loc[30, "market_cap"] = 0.0

    builder = FeatureBuilder(future_horizon=24, include_target=True)
    result = builder.build(df)

    assert (result["market_cap"] > 0).all()


def test_single_missing_raw_value_is_forward_filled() -> None:
    df = make_market_data(periods=60)

    missing_timestamp = df.loc[30, "timestamp"]
    previous_price = df.loc[29, "price"]

    df.loc[30, "price"] = np.nan

    builder = FeatureBuilder(future_horizon=24, include_target=False)
    result = builder.build(df)

    filled_row = result[result["timestamp"] == missing_timestamp]

    assert not filled_row.empty
    assert filled_row.iloc[0]["price"] == previous_price


def test_consecutive_missing_raw_values_are_not_fully_forward_filled() -> None:
    df = make_market_data(periods=60)

    df.loc[30, "price"] = np.nan
    df.loc[31, "price"] = np.nan

    builder = FeatureBuilder(future_horizon=24, include_target=False)
    result = builder.build(df)

    assert df.loc[31, "timestamp"] not in set(result["timestamp"])
