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

RAW_BACKFILL_DIR = Path("data/raw/hourly")
SYNC_DAYS = 2


def batch_load_coin_data(n_coins: int, currency: str) -> None:
    RAW_BACKFILL_DIR.mkdir(parents=True, exist_ok=True)

    client = CoinGeckoClient(base_url=BASE_URL, endpoint=ENDPOINT)
    
    ending_date = datetime.now(UTC)
    starting_date = ending_date - timedelta(days=SYNC_DAYS)

    coin_ids = client.get_coin_ids(number_of_coins=n_coins)

    for coin_id in tqdm(coin_ids, total=len(coin_ids), desc="Syncing Coin data from the last two days"):
        url = client.build_url(coin_id)

        params = client.build_params(
            vs_currency=currency, 
            starting_date=starting_date, 
            ending_date=ending_date
            )
        
        try:
            data = client.fetch_coin_data(url, params, headers)
        except RuntimeError as e:
            print(f"Could not fetch data for {coin_id}, cause: {e}")
            continue
        
        file_path = RAW_BACKFILL_DIR / (
            f"{coin_id}_{starting_date:%Y.%m.%d}_{ending_date:%Y.%m.%d}.json"
            )
            
        with open(file_path,"w",) as f:
            json.dump(data, f, indent=2)
            
    
    print(f"Successfully fetched hourly data of the two days for {len(coin_ids)} crypto-coins")
        

if __name__ == "__main__":
    batch_load_coin_data()

