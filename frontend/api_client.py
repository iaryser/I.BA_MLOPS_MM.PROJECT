import pandas as pd
import requests

from inference.schemas import PredictionRequest, PredictionResponse, TopCoin

API_URL = "http://127.0.0.1:8000"

HEALTH_URL = f"{API_URL}/health"
COINS_URL = f"{API_URL}/coins"
TOP5_URL = f"{API_URL}/top5_coins"
COIN_CONTEXT_URL = f"{API_URL}/coin_context"
PREDICT_URL = f"{API_URL}/predict"


def check_server_health() -> bool:
    try:
        res = requests.get(HEALTH_URL, timeout=5)
        res.raise_for_status()
        return res.json().get("status") == "ok"
    except requests.RequestException:
        return False


def get_top5_coins() -> list[TopCoin]:
    res = requests.get(TOP5_URL, timeout=10)
    res.raise_for_status()
    return [TopCoin(**coin) for coin in res.json()]


def get_available_coins() -> list[str]:
    res = requests.get(COINS_URL, timeout=10)
    res.raise_for_status()
    return res.json()


def get_coin_context(coin_id: str, n_days: int = 30) -> pd.DataFrame:
    res = requests.get(
        COIN_CONTEXT_URL,
        params={"coin_id": coin_id, "n_days": n_days},
        timeout=10,
    )
    res.raise_for_status()

    df = pd.DataFrame(res.json())

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp")


def predict_coin(coin_id: str) -> PredictionResponse:
    req = PredictionRequest(coin_id=coin_id)

    res = requests.post(
        PREDICT_URL,
        json=req.model_dump(),
        timeout=10,
    )
    res.raise_for_status()

    return PredictionResponse(**res.json())
