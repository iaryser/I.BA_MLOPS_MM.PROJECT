import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from feature_pipeline.transforms import build_staging_table as staging


def write_raw_coin_json(
    directory: Path,
    filename: str,
    prices: list[list[float]],
    market_caps: list[list[float]],
    total_volumes: list[list[float]],
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)

    file_path = directory / filename
    file_path.write_text(
        json.dumps(
            {
                "prices": prices,
                "market_caps": market_caps,
                "total_volumes": total_volumes,
            }
        )
    )

    return file_path


def test_transform_json_to_rows_converts_raw_json_to_rows(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"

    write_raw_coin_json(
        directory=raw_dir,
        filename="bitcoin_2026_01_01.json",
        prices=[
            [1767225600000, 100.0],
            [1767229200000, 101.0],
        ],
        market_caps=[
            [1767225600000, 1_000_000.0],
            [1767229200000, 1_010_000.0],
        ],
        total_volumes=[
            [1767225600000, 50_000.0],
            [1767229200000, 51_000.0],
        ],
    )

    result = staging.transform_json_to_rows(raw_dir)

    assert result == [
        {
            "coin_id": "bitcoin",
            "timestamp": datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            "price": 100.0,
            "market_cap": 1_000_000.0,
            "volume": 50_000.0,
        },
        {
            "coin_id": "bitcoin",
            "timestamp": datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
            "price": 101.0,
            "market_cap": 1_010_000.0,
            "volume": 51_000.0,
        },
    ]


def test_transform_json_to_rows_uses_coin_id_from_filename(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"

    write_raw_coin_json(
        directory=raw_dir,
        filename="ethereum_some_snapshot.json",
        prices=[[1767225600000, 200.0]],
        market_caps=[[1767225600000, 2_000_000.0]],
        total_volumes=[[1767225600000, 60_000.0]],
    )

    result = staging.transform_json_to_rows(raw_dir)

    assert result[0]["coin_id"] == "ethereum"


def test_transform_json_to_rows_rounds_timestamp_down_to_full_hour(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"

    write_raw_coin_json(
        directory=raw_dir,
        filename="bitcoin_snapshot.json",
        prices=[[1767227400123, 100.0]],  # 2026-01-01 00:30:00.123 UTC
        market_caps=[[1767227400123, 1_000_000.0]],
        total_volumes=[[1767227400123, 50_000.0]],
    )

    result = staging.transform_json_to_rows(raw_dir)

    assert result[0]["timestamp"] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def test_transform_json_to_rows_uses_shortest_available_array_length(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"

    write_raw_coin_json(
        directory=raw_dir,
        filename="bitcoin_snapshot.json",
        prices=[
            [1767225600000, 100.0],
            [1767229200000, 101.0],
            [1767232800000, 102.0],
        ],
        market_caps=[
            [1767225600000, 1_000_000.0],
            [1767229200000, 1_010_000.0],
        ],
        total_volumes=[
            [1767225600000, 50_000.0],
            [1767229200000, 51_000.0],
            [1767232800000, 52_000.0],
        ],
    )

    result = staging.transform_json_to_rows(raw_dir)

    assert len(result) == 2
    assert result[-1]["price"] == 101.0


def test_transform_json_to_rows_skips_file_with_empty_series(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"

    write_raw_coin_json(
        directory=raw_dir,
        filename="bitcoin_snapshot.json",
        prices=[],
        market_caps=[[1767225600000, 1_000_000.0]],
        total_volumes=[[1767225600000, 50_000.0]],
    )

    result = staging.transform_json_to_rows(raw_dir)

    assert result == []


def test_add_missing_hourly_rows_adds_nan_row_for_missing_hour() -> None:
    df = pd.DataFrame(
        {
            "coin_id": ["bitcoin", "bitcoin"],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 02:00:00+00:00",
                ]
            ),
            "price": [100.0, 102.0],
            "market_cap": [1_000_000.0, 1_020_000.0],
            "volume": [50_000.0, 52_000.0],
        }
    )

    result = staging.add_missing_hourly_rows(df)

    assert len(result) == 3

    missing_row = result[
        result["timestamp"] == pd.Timestamp("2026-01-01 01:00:00", tz="UTC")
    ].iloc[0]

    assert missing_row["coin_id"] == "bitcoin"
    assert pd.isna(missing_row["price"])
    assert pd.isna(missing_row["market_cap"])
    assert pd.isna(missing_row["volume"])


def test_add_missing_hourly_rows_does_not_mix_coins() -> None:
    df = pd.DataFrame(
        {
            "coin_id": ["bitcoin", "bitcoin", "ethereum", "ethereum"],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 02:00:00+00:00",
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                ]
            ),
            "price": [100.0, 102.0, 200.0, 201.0],
            "market_cap": [1_000_000.0, 1_020_000.0, 2_000_000.0, 2_010_000.0],
            "volume": [50_000.0, 52_000.0, 60_000.0, 61_000.0],
        }
    )

    result = staging.add_missing_hourly_rows(df)

    bitcoin = result[result["coin_id"] == "bitcoin"]
    ethereum = result[result["coin_id"] == "ethereum"]

    assert len(bitcoin) == 3
    assert len(ethereum) == 2


def test_load_market_data_returns_empty_dataframe_when_file_does_not_exist(
    tmp_path: Path,
) -> None:
    result = staging.load_market_data(tmp_path / "missing.parquet")

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_load_market_data_reads_existing_parquet_file(tmp_path: Path) -> None:
    file_path = tmp_path / "market_data.parquet"
    expected = pd.DataFrame(
        {
            "coin_id": ["bitcoin"],
            "timestamp": pd.to_datetime(["2026-01-01 00:00:00+00:00"]),
            "price": [100.0],
            "market_cap": [1_000_000.0],
            "volume": [50_000.0],
        }
    )

    expected.to_parquet(file_path, index=False)

    result = staging.load_market_data(file_path)

    pd.testing.assert_frame_equal(result, expected)


def test_build_staging_table_rejects_invalid_source() -> None:
    with pytest.raises(ValueError, match="Invalid source"):
        staging.build_staging_table(source="invalid")


def test_build_staging_table_builds_backfill_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_path = tmp_path / "staging" / "market_data.parquet"
    backfill_dir = tmp_path / "raw" / "backfill"
    hourly_dir = tmp_path / "raw" / "hourly"

    write_raw_coin_json(
        directory=backfill_dir,
        filename="bitcoin_snapshot.json",
        prices=[
            [1767225600000, 100.0],
            [1767229200000, 101.0],
        ],
        market_caps=[
            [1767225600000, 1_000_000.0],
            [1767229200000, 1_010_000.0],
        ],
        total_volumes=[
            [1767225600000, 50_000.0],
            [1767229200000, 51_000.0],
        ],
    )

    monkeypatch.setattr(staging, "DATA_PATH", data_path)
    monkeypatch.setattr(staging, "BACKFILL_DIR", backfill_dir)
    monkeypatch.setattr(staging, "HOURLY_DIR", hourly_dir)

    staging.build_staging_table(source="backfill")

    assert data_path.exists()

    result = pd.read_parquet(data_path)

    expected = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                ]
            ),
            "coin_id": ["bitcoin", "bitcoin"],
            "price": [100.0, 101.0],
            "market_cap": [1_000_000.0, 1_010_000.0],
            "volume": [50_000.0, 51_000.0],
        }
    )

    pd.testing.assert_frame_equal(result, expected)


def test_build_staging_table_batch_keeps_latest_duplicate_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_path = tmp_path / "staging" / "market_data.parquet"
    backfill_dir = tmp_path / "raw" / "backfill"
    hourly_dir = tmp_path / "raw" / "hourly"

    existing = pd.DataFrame(
        {
            "coin_id": ["bitcoin"],
            "timestamp": pd.to_datetime(["2026-01-01 00:00:00+00:00"]),
            "price": [100.0],
            "market_cap": [1_000_000.0],
            "volume": [50_000.0],
        }
    )
    data_path.parent.mkdir(parents=True, exist_ok=True)
    existing.to_parquet(data_path, index=False)

    write_raw_coin_json(
        directory=hourly_dir,
        filename="bitcoin_snapshot.json",
        prices=[[1767225600000, 999.0]],
        market_caps=[[1767225600000, 9_990_000.0]],
        total_volumes=[[1767225600000, 999_000.0]],
    )

    monkeypatch.setattr(staging, "DATA_PATH", data_path)
    monkeypatch.setattr(staging, "BACKFILL_DIR", backfill_dir)
    monkeypatch.setattr(staging, "HOURLY_DIR", hourly_dir)

    staging.build_staging_table(source="batch")

    result = pd.read_parquet(data_path)

    assert len(result) == 1
    assert result.iloc[0]["price"] == 999.0
    assert result.iloc[0]["market_cap"] == 9_990_000.0
    assert result.iloc[0]["volume"] == 999_000.0
