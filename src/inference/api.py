import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, Request

from inference.inference_logger import InferenceLogger
from inference.market_context_loader import MarketContextLoader
from inference.model_loader import ModelLoader
from inference.online_feature_loader import OnlineFeatureLoader
from inference.prediction_service import PredictionService
from inference.schemas import PredictionRequest, PredictionResponse, TopCoin

ONLINE_FEATURE_PATH = os.getenv("ONLINE_FEATURE_PATH")
MARKET_DATA_PATH = os.getenv("MARKET_DATA_PATH")
BUCKET_NAME = os.getenv("BUCKET_NAME")

DATA_CACHE_TTL = timedelta(hours=1)


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

    app.state.data_loaded_at = datetime.now(UTC)

    yield


app = FastAPI(
    title="Crypto Direction Prediction API",
    lifespan=lifespan,
)


def refresh_data_if_stale(app: FastAPI) -> None:
    loaded_at = app.state.data_loaded_at

    if datetime.now(UTC) - loaded_at < DATA_CACHE_TTL:
        return

    app.state.online_feature_loader.reload()
    app.state.market_data_loader.reload()
    app.state.data_loaded_at = datetime.now(UTC)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/coins")
def get_coins(request: Request) -> list[str]:
    refresh_data_if_stale(request.app)
    return request.app.state.online_feature_loader.get_available_coins()


@app.get("/top5_coins")
def get_top5_coins(request: Request) -> list[TopCoin]:
    refresh_data_if_stale(request.app)
    return request.app.state.online_feature_loader.load_top5_coins()


@app.get("/coin_context")
def get_coin_metadata(request: Request, coin_id: str, n_days: int) -> list[dict]:
    refresh_data_if_stale(request.app)
    return request.app.state.market_data_loader.load_coin_context_data(
        coin_id=coin_id, n_days=n_days
    )


@app.post("/predict")
def predict(request: Request, req: PredictionRequest) -> PredictionResponse:
    refresh_data_if_stale(request.app)
    return request.app.state.prediction_service.predict(req.coin_id)
