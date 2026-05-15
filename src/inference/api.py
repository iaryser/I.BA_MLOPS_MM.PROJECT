from pathlib import Path

from fastapi import FastAPI

from inference.model_loader import ModelLoader
from inference.market_context_loader import MarketContextLoader
from inference.online_feature_loader import OnlineFeatureLoader
from inference.prediction_service import PredictionService
from inference.schemas import PredictionRequest, PredictionResponse, TopCoin

app = FastAPI(title="Crypto Direction Prediction API")


ONLINE_FEATURE_PATH = Path("data/online_store/online_features.parquet")
MARKET_DATA_PATH = Path("data/staging/market_data.parquet")

online_feature_loader = OnlineFeatureLoader(data_path=ONLINE_FEATURE_PATH)
market_data_loader = MarketContextLoader(data_path=MARKET_DATA_PATH)


model_loader = ModelLoader(
    artifact_name="xgboost-direction-model",
    alias="production",
    model_name="xgboost_model")

prediction_service = PredictionService(
    model_loader=model_loader,
    feature_loader=online_feature_loader
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/coins")
def get_coins() -> list[str]:
    return online_feature_loader.get_available_coins()


@app.get("/top5_coins")
def get_top5_coins() -> list[TopCoin]:
    return online_feature_loader.load_top5_coins()


@app.get("/coin_context")
def get_coin_metadata(coin_id: str, n_days: int) -> list[dict]:
    return market_data_loader.load_coin_context_data(coin_id=coin_id, n_days=n_days)


@app.post("/predict")
def predict(req: PredictionRequest) -> PredictionResponse:
    return prediction_service.predict(req.coin_id)