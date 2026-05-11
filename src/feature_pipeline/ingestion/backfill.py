import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from feature_pipeline.ingestion.coingecko_client import CoinGeckoClient

load_dotenv()
API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set")


BASE_URL = "https://api.coingecko.com/api/v3/coins"
ENDPOINT = "market_chart/range"
headers = {"x-cg-demo-api-key": API_KEY}

RAW_BACKFILL_DIR = Path("data/raw/backfill")

BACKFILL_DAYS = 365


def backfill_coin_data(n_coins: int, currency: str) -> None:
    RAW_BACKFILL_DIR.mkdir(parents=True, exist_ok=True)

    client = CoinGeckoClient(base_url=BASE_URL, endpoint=ENDPOINT)
    
    ending_date = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    starting_date = ending_date - timedelta(days=BACKFILL_DAYS)

    chunks = client.build_chunks(starting_date, ending_date)

    coin_ids = client.get_coin_ids(number_of_coins=n_coins)

    for coin_id in tqdm(coin_ids, total=len(coin_ids), desc="Backfilling coin data"):
        url = client.build_url(coin_id)

        for chunk_start, chunk_end in chunks:
            
            params = client.build_params(
                vs_currency=currency,
                starting_date=chunk_start, 
                ending_date=chunk_end
                )

            try:
                data = client.fetch_coin_data(url, params, headers)
            except RuntimeError as e:
                print(
                    f"Could not fetch data for {coin_id}. "
                    f"in interval {chunk_start:%Y-%m-%d} to {chunk_end:%Y-%m-%d}. "
                    f"cause: {e}"
                )
                continue
            
            file_path = RAW_BACKFILL_DIR / (
                f"{coin_id}_{chunk_start:%Y.%m.%d}_{chunk_end:%Y.%m.%d}.json"
                )
            
            with open(file_path,"w",) as f:
                json.dump(data, f, indent=2)
    
    print(f"Successfully fetched hourly data of the last year for {len(coin_ids)} crypto-coins")
        

if __name__ == "__main__":
    backfill_coin_data()
