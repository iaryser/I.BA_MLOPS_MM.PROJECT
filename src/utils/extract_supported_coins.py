import json

import requests

URL = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false",
}

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

with open("data/raw/market_snapshot/top100_coins.json", "w") as f:
    json.dump(coin_dict, f, indent=2)
