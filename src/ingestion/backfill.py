import json
import os
from dotenv import load_dotenv
import time
from datetime import UTC, datetime, timedelta

import requests
from requests.exceptions import HTTPError

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set")


BASE_URL = "https://api.coingecko.com/api/v3/coins"

ENDPOINT = "market_chart/range"

headers = {"x-cg-demo-api-key": API_KEY}



def _datetime_to_unix(dt: datetime) -> str:
    dt = int(dt.timestamp())  # cast to int to get rid of float
    return str(dt)


def build_params(vs_currency: str, starting_date: datetime, ending_date: datetime) -> dict:
    params = {
        "vs_currency": vs_currency,
        "from": _datetime_to_unix(starting_date),
        "to": _datetime_to_unix(ending_date),
        "precision": "full",  # always want full precision of price-values
        "interval": "hourly",  # always want hourly data
    }
    return params


def build_url(coin_type: str) -> str:
    return f"{BASE_URL}/{coin_type}/{ENDPOINT}"


def build_chunks(starting_date: datetime, ending_date: datetime) -> list[tuple[datetime, datetime]]:
    start = starting_date
    end = ending_date

    chunks = []

    while start < end:
        if (end - start) < timedelta(days=100):
            curr_end = end
        else:
            curr_end = start + timedelta(days=100)

        chunks.append((start, curr_end))
        start = curr_end

    return chunks


def get_coin_ids(number_of_coins: int) -> list[str]:
    coin_id_list = []

    with open("data/raw/market_snapshot/top100_coins.json") as f:
        data = json.load(f)

    counter = 0
    for coin_id in data.keys():
        if counter == number_of_coins:
            break

        coin_id_list.append(coin_id)
        counter += 1

    return coin_id_list


def fetch_coin_data(url: str, params: dict, n_retries: int) -> dict:
    last_error = None
    attempt = 0

    while attempt < n_retries:
        try:
            response = requests.get(url=url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()

        except HTTPError as e:
            last_error = e
            attempt += 1

            if e.response is not None and e.response.status_code == 429:
                print("Rate limited. Waiting 60s...")
                time.sleep(60)  # we hit the rate limit (should reset after 60 seconds)

            print(f"HTTPError occurred: {e}")

    raise RuntimeError("Max retries exceeded") from last_error


def main() -> None:
    ending_date = datetime.now(UTC)
    starting_date = ending_date - timedelta(days=365)

    chunks = build_chunks(starting_date, ending_date)

    coin_ids = get_coin_ids(number_of_coins=5)

    for coin_id in coin_ids:
        url = build_url(coin_id)

        for chunk_start, chunk_end in chunks:
            params = build_params("chf", chunk_start, chunk_end)

            data = fetch_coin_data(url, params, n_retries=5)

            with open(
                f"data/raw/backfill/{coin_id}_{chunk_start:%Y.%m.%d.}_{chunk_end:%Y.%m.%d.}.json",
                "w",
            ) as f:
                json.dump(data, f, indent=2)

        print(f"Successfully fetched 1 year of hourly historical data for {coin_id}.")
    
    print(f"Successfully fetched hourly data of the last year for {len(coin_ids)}")
        

if __name__ == "__main__":
    main()
