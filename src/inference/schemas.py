from typing import Any

from pydantic import BaseModel


class PredictionRequest(BaseModel):
    coin_id: str


class PredictionResponse(BaseModel):
    coin_id: str
    timestamp: str
    prediciton: int
    direction: str
    probability_up: float
    model_alias: str


class LoadedModel(BaseModel):
    model: Any
    version: str
    alias: str


class TopCoin(BaseModel):
    coin_id: str
    volume: float
