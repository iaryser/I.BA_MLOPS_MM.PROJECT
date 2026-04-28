import json
import time
from datetime import datetime, timedelta

import requests
from requests.exceptions import HTTPError


class CoinGeckoClient:
    
    def __init__(self, base_url: str, endpoint: str, timeout: int = 10, n_retries: int = 3) -> None:
        self.base_url = base_url
        self.endpoint = endpoint
        self.timeout = timeout
        self.n_retries = n_retries
        
        
    def _datetime_to_unix(self, dt: datetime) -> str:
        dt = int(dt.timestamp())  # cast to int to get rid of float
        return str(dt)


    def build_params(self, vs_currency: str, starting_date: datetime, ending_date: datetime) -> dict:
        params = {
            "vs_currency": vs_currency,
            "from": self._datetime_to_unix(starting_date),
            "to": self._datetime_to_unix(ending_date),
            "precision": "full",  # always want full precision of price-values
            "interval": "hourly",  # always want hourly data
        }
        return params


    def build_url(self, coin_type: str) -> str:
        return f"{self.base_url}/{coin_type}/{self.endpoint}"


    def build_chunks(self, starting_date: datetime, ending_date: datetime) -> list[tuple[datetime, datetime]]:
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


    def get_coin_ids(self, number_of_coins: int) -> list[str]:
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


    def fetch_coin_data(self, url: str, params: dict, headers: dict) -> dict:
        last_error = None
        attempt = 0

        while attempt < self.n_retries:
            try:
                response = requests.get(url=url, params=params, headers=headers, timeout=self.timeout)
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