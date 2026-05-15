import json
from pathlib import Path

import requests

FILEPATH = Path("data/reference")
FILEPATH.mkdir(parents=True, exist_ok=True)

URL = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false",
}


def main() -> None:
    res = requests.get(URL, params=params, timeout=10)
    data = res.json()

    coin_dict = {}

    for coin in data:
        coin_dict[coin["id"]] = {
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price": coin["current_price"],
            "rank": coin["market_cap_rank"],
        }

    with open(FILEPATH / "top100_coins.json", "w") as f:
        json.dump(coin_dict, f, indent=2)

if __name__ == "__main__":
    main()
