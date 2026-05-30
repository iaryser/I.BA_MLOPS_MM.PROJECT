import numpy as np
import pandas as pd
import pytest

from inference import prediction_service
from inference.prediction_service import PredictionService
from inference.schemas import LoadedModel


class FakeModel:
    def predict_proba(self, features: pd.DataFrame):
        assert list(features.columns) == ["return", "return_6"]
        assert features.iloc[0]["return"] == 0.01
        return np.array([[0.25, 0.75]])


class FakeModelLoader:
    def get_model(self) -> LoadedModel:
        return LoadedModel(
            model=FakeModel(),
            version="v17",
            alias="production",
        )


class FakeFeatureLoader:
    def load_features(self, coin_id: str) -> pd.DataFrame:
        assert coin_id == "bitcoin"
        return pd.DataFrame(
            {
                "return": [0.01],
                "return_6": [0.05],
            }
        )
        
    def load_context(self, coin_id: str) -> dict:
        assert coin_id == "bitcoin"
        return {
            "coin_id": "bitcoin",
            "timestamp": pd.Timestamp("2026-01-01 12:00:00", tz="UTC"),
            "price": 100.0,
            "market_cap": 1_000_000.0,
            "volume": 50_000.0,
        }
        
class FakeLogger:
    def __init__(self) -> None:
        self.logged_events = []

    def log(self, event: dict) -> None:
        self.logged_events.append(event)


def test_prediction_service_returns_prediction_response() -> None:
    service = PredictionService(
        model_loader=FakeModelLoader(),
        feature_loader=FakeFeatureLoader(),
        logger=FakeLogger()
    )

    result = service.predict("bitcoin")

    assert result.coin_id == "bitcoin"
    assert result.timestamp == "2026-01-01 12:00:00"
    assert result.prediction == 1
    assert result.direction == "up"
    assert result.probability_up == 0.75
    assert result.model_alias == "production"


def test_prediction_service_returns_down_when_probability_is_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDownPredictor:
        def __init__(self, model) -> None:
            self.model = model

        def execute_prediction(self, features: pd.DataFrame) -> dict:
            return {
                "y_proba": 0.25,
                "y_pred": 0,
                "direction": "down",
            }

    monkeypatch.setattr(prediction_service, "Predictor", FakeDownPredictor)

    service = PredictionService(
        model_loader=FakeModelLoader(),
        feature_loader=FakeFeatureLoader(),
        logger=FakeLogger()
    )

    result = service.predict("bitcoin")

    assert result.prediction == 0
    assert result.direction == "down"
    assert result.probability_up == 0.25


def test_prediction_service_uses_requested_coin_id() -> None:
    class TrackingFeatureLoader:
        def __init__(self) -> None:
            self.feature_coin_id = None
            self.context_coin_id = None

        def load_features(self, coin_id: str) -> pd.DataFrame:
            self.feature_coin_id = coin_id
            return pd.DataFrame({"return": [0.01], "return_6": [0.05]})

        def load_context(self, coin_id: str) -> dict:
            self.context_coin_id = coin_id
            return {
                "timestamp": pd.Timestamp("2026-01-01 12:00:00", tz="UTC"),
            }

    feature_loader = TrackingFeatureLoader()

    service = PredictionService(
        model_loader=FakeModelLoader(),
        feature_loader=feature_loader,
        logger=FakeLogger()
    )

    service.predict("ethereum")

    assert feature_loader.feature_coin_id == "ethereum"
    assert feature_loader.context_coin_id == "ethereum"


def test_prediction_service_propagates_feature_loader_error() -> None:
    class BrokenFeatureLoader:
        def load_features(self, coin_id: str) -> pd.DataFrame:
            raise IndexError("coin not found")

        def load_context(self, coin_id: str) -> dict:
            return {}

    service = PredictionService(
        model_loader=FakeModelLoader(),
        feature_loader=BrokenFeatureLoader(),
        logger=FakeLogger()
    )

    with pytest.raises(IndexError, match="coin not found"):
        service.predict("unknown-coin")


def test_prediction_service_propagates_model_loader_error() -> None:
    class BrokenModelLoader:
        def get_model(self) -> LoadedModel:
            raise RuntimeError("model could not be loaded")

    service = PredictionService(
        model_loader=BrokenModelLoader(),
        feature_loader=FakeFeatureLoader(),
        logger=FakeLogger()
    )

    with pytest.raises(RuntimeError, match="model could not be loaded"):
        service.predict("bitcoin")
