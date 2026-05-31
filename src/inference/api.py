import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from inference.inference_logger import InferenceLogger
from inference.market_context_loader import MarketContextLoader
from inference.model_loader import ModelLoader
from inference.online_feature_loader import OnlineFeatureLoader
from inference.prediction_service import PredictionService
from inference.schemas import PredictionRequest, PredictionResponse, TopCoin

ONLINE_FEATURE_PATH = os.getenv("ONLINE_FEATURE_PATH")
MARKET_DATA_PATH = os.getenv("MARKET_DATA_PATH")
LOG_DIR = os.getenv("LOG_DIR")
BUCKET_NAME = os.getenv("BUCKET_NAME")


@asynccontextmanager
async def lifespan(app: FastAPI):
    online_feature_loader = OnlineFeatureLoader(data_path=ONLINE_FEATURE_PATH)
    market_data_loader = MarketContextLoader(data_path=MARKET_DATA_PATH)

    model_loader = ModelLoader(
        artifact_name="xgboost-direction-model",
        alias="production",
        model_name="xgboost_model",
    )

    app.state.online_feature_loader = online_feature_loader
    app.state.market_data_loader = market_data_loader
    app.state.prediction_service = PredictionService(
        model_loader=model_loader,
        feature_loader=online_feature_loader,
        logger=InferenceLogger(BUCKET_NAME, "logs/inference"),
    )

    yield


app = FastAPI(
    title="Crypto Direction Prediction API",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/coins")
def get_coins(request: Request) -> list[str]:
    return request.app.state.online_feature_loader.get_available_coins()


@app.get("/top5_coins")
def get_top5_coins(request: Request) -> list[TopCoin]:
    return request.app.state.online_feature_loader.load_top5_coins()


@app.get("/coin_context")
def get_coin_metadata(request: Request, coin_id: str, n_days: int) -> list[dict]:
    return request.app.state.market_data_loader.load_coin_context_data(
        coin_id=coin_id, n_days=n_days
    )


@app.post("/predict")
def predict(request: Request, req: PredictionRequest) -> PredictionResponse:
    return request.app.state.prediction_service.predict(req.coin_id)


@app.get("/reload-data")
def reload_data(request: Request) -> dict[str, str]:
    request.app.state.online_feature_loader.reload()
    request.app.state.market_data_loader.reload()
    return {"status": "reloaded"}
