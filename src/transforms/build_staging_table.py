from pathlib import Path
import json
import pandas as pd
from datetime import datetime, UTC

DATA_PATH = Path("data/staging/market_data.parquet")

BACKFILL_DIR = Path("data/raw/backfill")
HOURLY_DIR = Path("data/raw/hourly")


def transform_json_to_rows(directory: Path) -> list[dict[str]]:
    rows = []

    for file in sorted(directory.glob("*.json")):
        coin_id = file.name.split("_")[0]
        
        with open(file, "r") as f:
            data = json.load(f)
        
        prices = data["prices"]
        m_caps = data["market_caps"]
        volumes= data["total_volumes"]
        
        for i in range(len(prices)):
            rows.append({
                "coin_id": coin_id,
                "timestamp": datetime.fromtimestamp(prices[i][0] / 1000, UTC).replace(
                    minute=0,
                    second=0,
                    microsecond=0),
                "price": prices[i][1],
                "market_cap": m_caps[i][1],
                "volume": volumes[i][1],
            })
            
    return rows
            
            
def load_market_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    
    return pd.read_parquet(DATA_PATH)


def main() -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    market_df = load_market_data(DATA_PATH)
    
    backfill_data = pd.DataFrame(transform_json_to_rows(BACKFILL_DIR))
    hourly_data = pd.DataFrame(transform_json_to_rows(HOURLY_DIR))
    
    market_df = pd.concat(
        [market_df, backfill_data, hourly_data]
    )
    
    market_df = market_df.drop_duplicates(
        subset=["coin_id", "timestamp"],
        keep="last"
        )
    market_df = market_df.sort_values(["coin_id", "timestamp"])
    
    
    market_df.to_parquet(DATA_PATH, index=False)
    
    
if __name__ == "__main__":
    main()
