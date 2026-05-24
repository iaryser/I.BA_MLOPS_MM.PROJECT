import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

DATA_PATH = Path("data/staging/market_data.parquet")

BACKFILL_DIR = Path("data/raw/backfill")
HOURLY_DIR = Path("data/raw/hourly")


def transform_json_to_rows(directory: Path) -> list[dict[str]]:
    rows = []

    for file in sorted(directory.glob("*.json")):
        coin_id = file.name.split("_")[0]

        with open(file) as f:
            data = json.load(f)

        prices = data["prices"]
        m_caps = data["market_caps"]
        volumes = data["total_volumes"]

        if not prices or not m_caps or not volumes:
            continue

        min_len = min(len(prices), len(m_caps), len(volumes))

        for i in range(min_len):
            rows.append(
                {
                    "coin_id": coin_id,
                    "timestamp": datetime.fromtimestamp(
                        prices[i][0] / 1000, UTC
                    ).replace(minute=0, second=0, microsecond=0),
                    "price": prices[i][1],
                    "market_cap": m_caps[i][1],
                    "volume": volumes[i][1],
                }
            )

    return rows


def add_missing_hourly_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["coin_id", "timestamp"]).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    completed = []

    for coin_id, group in df.groupby("coin_id"):
        group = group.set_index("timestamp").sort_index()

        full_index = pd.date_range(
            start=group.index.min(),
            end=group.index.max(),
            freq="1h",
            tz="UTC",
        )

        group = group.reindex(full_index)
        group.index.name = "timestamp"
        group["coin_id"] = coin_id

        completed.append(group.reset_index())

    return pd.concat(completed, ignore_index=True)


def load_market_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    return pd.read_parquet(path)


def build_staging_table(source: str = "batch") -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    if source not in {"backfill", "batch"}:
        raise ValueError("Invalid source")

    market_df = load_market_data(DATA_PATH)

    if source == "backfill":
        backfill_data = pd.DataFrame(transform_json_to_rows(BACKFILL_DIR))
        market_df = pd.concat([market_df, backfill_data])

    if source == "batch":
        hourly_data = pd.DataFrame(transform_json_to_rows(HOURLY_DIR))
        market_df = pd.concat([market_df, hourly_data])

    market_df = market_df.drop_duplicates(subset=["coin_id", "timestamp"], keep="last")

    market_df = add_missing_hourly_rows(market_df)

    market_df.to_parquet(DATA_PATH, index=False)


if __name__ == "__main__":
    build_staging_table()
