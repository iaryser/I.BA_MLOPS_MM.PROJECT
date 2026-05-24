from fastapi.testclient import TestClient

from inference import api
from inference.schemas import PredictionResponse, TopCoin

client = TestClient(api.app)


class FakeOnlineFeatureLoader:
    def __init__(self) -> None:
        self.reloaded = False

    def get_available_coins(self) -> list[str]:
        return ["bitcoin", "ethereum"]

    def load_top5_coins(self) -> list[TopCoin]:
        return [
            TopCoin(coin_id="bitcoin", volume=100_000.0),
            TopCoin(coin_id="ethereum", volume=90_000.0),
        ]

    def reload(self) -> None:
        self.reloaded = True


class FakeMarketDataLoader:
    def __init__(self) -> None:
        self.reloaded = False

    def load_coin_context_data(self, coin_id: str, n_days: int) -> list[dict]:
        return [
            {
                "coin_id": coin_id,
                "timestamp": "2026-01-01T00:00:00Z",
                "price": 100.0,
                "volume": 50_000.0,
                "market_cap": 1_000_000.0,
            }
        ]

    def reload(self) -> None:
        self.reloaded = True


class FakePredictionService:
    def predict(self, coin_id: str) -> PredictionResponse:
        return PredictionResponse(
            coin_id=coin_id,
            timestamp="2026-01-01 12:00:00",
            prediciton=1,
            direction="up",
            probability_up=0.75,
            model_alias="production",
        )


def test_health_endpoint_returns_ok() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_coins_endpoint_returns_available_coins(monkeypatch) -> None:
    monkeypatch.setattr(api, "online_feature_loader", FakeOnlineFeatureLoader())

    response = client.get("/coins")

    assert response.status_code == 200
    assert response.json() == ["bitcoin", "ethereum"]


def test_top5_coins_endpoint_returns_top_coins(monkeypatch) -> None:
    monkeypatch.setattr(api, "online_feature_loader", FakeOnlineFeatureLoader())

    response = client.get("/top5_coins")

    assert response.status_code == 200
    assert response.json() == [
        {"coin_id": "bitcoin", "volume": 100_000.0},
        {"coin_id": "ethereum", "volume": 90_000.0},
    ]


def test_coin_context_endpoint_returns_context_data(monkeypatch) -> None:
    monkeypatch.setattr(api, "market_data_loader", FakeMarketDataLoader())

    response = client.get("/coin_context?coin_id=bitcoin&n_days=7")

    assert response.status_code == 200
    assert response.json() == [
        {
            "coin_id": "bitcoin",
            "timestamp": "2026-01-01T00:00:00Z",
            "price": 100.0,
            "volume": 50_000.0,
            "market_cap": 1_000_000.0,
        }
    ]


def test_predict_endpoint_returns_prediction(monkeypatch) -> None:
    monkeypatch.setattr(api, "prediction_service", FakePredictionService())

    response = client.post(
        "/predict",
        json={"coin_id": "bitcoin"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "coin_id": "bitcoin",
        "timestamp": "2026-01-01 12:00:00",
        "prediciton": 1,
        "direction": "up",
        "probability_up": 0.75,
        "model_alias": "production",
    }


def test_predict_endpoint_rejects_missing_coin_id() -> None:
    response = client.post(
        "/predict",
        json={},
    )

    assert response.status_code == 422


def test_reload_data_endpoint_reloads_both_loaders(monkeypatch) -> None:
    fake_online_loader = FakeOnlineFeatureLoader()
    fake_market_loader = FakeMarketDataLoader()

    monkeypatch.setattr(api, "online_feature_loader", fake_online_loader)
    monkeypatch.setattr(api, "market_data_loader", fake_market_loader)

    response = client.get("/reload-data")

    assert response.status_code == 200
    assert response.json() == {"status": "reloaded"}
    assert fake_online_loader.reloaded is True
    assert fake_market_loader.reloaded is True
