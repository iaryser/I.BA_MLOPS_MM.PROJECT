import importlib

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from inference.schemas import PredictionResponse, TopCoin


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
            prediction=1,
            direction="up",
            probability_up=0.75,
            model_alias="production",
            model_version="v1",
        )


class FakeArtifact:
    version = "v3"

    def __init__(self, download_dir) -> None:
        self.download_dir = download_dir

    def download(self) -> str:
        return str(self.download_dir)


class FakeWandbApi:
    def __init__(self, download_dir) -> None:
        self.download_dir = download_dir

    def artifact(self, artifact_name: str) -> FakeArtifact:
        return FakeArtifact(self.download_dir)


class FakeModel:
    def predict_proba(self, features):
        return [[0.25, 0.75]]


class FakeBlob:
    def upload_from_string(self, data: str, content_type: str) -> None:
        pass


class FakeBucket:
    def blob(self, blob_name: str) -> FakeBlob:
        return FakeBlob()


class FakeStorageClient:
    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket()


def test_health_endpoint_returns_ok(client) -> None:
    test_client, _ = client

    response = test_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    online_features_path = tmp_path / "online_features.parquet"
    market_data_path = tmp_path / "market_data.parquet"

    fake_df = pd.DataFrame(
        {
            "coin_id": ["bitcoin", "ethereum"],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                ]
            ),
            "price": [100.0, 200.0],
            "market_cap": [1_000_000.0, 2_000_000.0],
            "volume": [100_000.0, 90_000.0],
            "return": [0.01, 0.02],
            "return_6": [0.03, 0.04],
            "return_12": [0.05, 0.06],
            "return_24": [0.07, 0.08],
            "ma_deviation_6": [0.01, 0.02],
            "ma_deviation_12": [0.01, 0.02],
            "ma_deviation_24": [0.01, 0.02],
            "volatility_6": [0.1, 0.2],
            "volatility_12": [0.1, 0.2],
            "volatility_24": [0.1, 0.2],
            "normalized_momentum_6": [0.01, 0.02],
            "normalized_momentum_12": [0.01, 0.02],
            "normalized_momentum_24": [0.01, 0.02],
            "log_volume_change_6": [0.01, 0.02],
            "log_volume_change_12": [0.01, 0.02],
            "log_volume_change_24": [0.01, 0.02],
            "volume_to_mcap": [0.1, 0.2],
        }
    )

    fake_df.to_parquet(online_features_path, index=False)
    fake_df.to_parquet(market_data_path, index=False)

    monkeypatch.setenv("ONLINE_FEATURE_PATH", str(online_features_path))
    monkeypatch.setenv("MARKET_DATA_PATH", str(market_data_path))
    monkeypatch.setenv("WANDB_API_KEY", "fake-api-key")
    monkeypatch.setenv("WANDB_ENTITY", "fake-entity")
    monkeypatch.setenv("WANDB_PROJECT", "fake-project")

    from inference import model_loader

    fake_api = FakeWandbApi(tmp_path)

    monkeypatch.setattr(model_loader.wandb, "Api", lambda: fake_api)
    monkeypatch.setattr(model_loader.joblib, "load", lambda path: FakeModel())

    from inference import inference_logger

    monkeypatch.setattr(
        inference_logger.storage,
        "Client",
        lambda: FakeStorageClient(),
    )

    import inference.api as api

    importlib.reload(api)

    return TestClient(api.app), api


def test_coins_endpoint_returns_available_coins(client, monkeypatch) -> None:
    test_client, api = client

    monkeypatch.setattr(api, "online_feature_loader", FakeOnlineFeatureLoader())

    response = test_client.get("/coins")

    assert response.status_code == 200
    assert response.json() == ["bitcoin", "ethereum"]


def test_top5_coins_endpoint_returns_top_coins(client, monkeypatch) -> None:
    test_client, api = client

    monkeypatch.setattr(api, "online_feature_loader", FakeOnlineFeatureLoader())

    response = test_client.get("/top5_coins")

    assert response.status_code == 200
    assert response.json() == [
        {"coin_id": "bitcoin", "volume": 100_000.0},
        {"coin_id": "ethereum", "volume": 90_000.0},
    ]


def test_coin_context_endpoint_returns_context_data(client, monkeypatch) -> None:
    test_client, api = client

    monkeypatch.setattr(api, "market_data_loader", FakeMarketDataLoader())

    response = test_client.get("/coin_context?coin_id=bitcoin&n_days=7")

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


def test_predict_endpoint_returns_prediction(client, monkeypatch) -> None:
    test_client, api = client
    monkeypatch.setattr(api, "prediction_service", FakePredictionService())

    response = test_client.post(
        "/predict",
        json={"coin_id": "bitcoin"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "coin_id": "bitcoin",
        "timestamp": "2026-01-01 12:00:00",
        "prediction": 1,
        "direction": "up",
        "probability_up": 0.75,
        "model_alias": "production",
        "model_version": "v1",
    }


def test_predict_endpoint_rejects_missing_coin_id(client) -> None:
    test_client, _ = client
    response = test_client.post(
        "/predict",
        json={},
    )

    assert response.status_code == 422


def test_reload_data_endpoint_reloads_both_loaders(client, monkeypatch) -> None:
    test_client, api = client

    fake_online_loader = FakeOnlineFeatureLoader()
    fake_market_loader = FakeMarketDataLoader()

    monkeypatch.setattr(api, "online_feature_loader", fake_online_loader)
    monkeypatch.setattr(api, "market_data_loader", fake_market_loader)

    response = test_client.get("/reload-data")

    assert response.status_code == 200
    assert response.json() == {"status": "reloaded"}
    assert fake_online_loader.reloaded is True
    assert fake_market_loader.reloaded is True
