from fastapi import FastAPI
from pathlib import Path

from online_feature_loader import OnlineFeatureLoader
from model_loader import ModelLoader
from prediction_service import PredictionService

from schemas import *


app = FastAPI(title="Crypto Direction Prediction API")


ONLINE_FEATURE_PATH = Path("data/online_store/online_features.parquet")

feature_loader = OnlineFeatureLoader(data_path=ONLINE_FEATURE_PATH)

model_loader = ModelLoader(
    artifact_name="xgboost-direction-model",
    alias="production",
    model_name="xgboost_model")

prediction_service = PredictionService(
    model_loader=model_loader,
    feature_loader=feature_loader
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/coins")
def get_coins() -> list[str]:
    return feature_loader.get_available_coins()


@app.post("/predict")
def predict(req: PredictionRequest) -> PredictionResponse:
    return prediction_service.predict(req.coin_id)