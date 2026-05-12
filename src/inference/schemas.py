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
    model_version: str
    
class LoadedModel(BaseModel):
    model: Any
    threshold: float
    version: str
    alias: str
    
class TopCoin(BaseModel):
    coin_id: str
    volume: float