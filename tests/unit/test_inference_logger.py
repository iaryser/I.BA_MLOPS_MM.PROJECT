import json

from inference.inference_logger import InferenceLogger


class FakeBlob:
    def __init__(self) -> None:
        self.uploaded_data = None
        self.content_type = None

    def upload_from_string(self, data: str, content_type: str) -> None:
        self.uploaded_data = data
        self.content_type = content_type


class FakeBucket:
    def __init__(self) -> None:
        self.blob_name = None
        self.fake_blob = FakeBlob()

    def blob(self, blob_name: str) -> FakeBlob:
        self.blob_name = blob_name
        return self.fake_blob


class FakeStorageClient:
    def __init__(self) -> None:
        self.bucket_name = None
        self.fake_bucket = FakeBucket()

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.bucket_name = bucket_name
        return self.fake_bucket


def test_inference_logger_uploads_prediction_event(monkeypatch) -> None:
    client = FakeStorageClient()

    monkeypatch.setattr(
        "inference.inference_logger.storage.Client",
        lambda: client,
    )

    logger = InferenceLogger(
        bucket_name="test-bucket",
        prefix="logs/inference",
    )

    event = {
        "coin_id": "bitcoin",
        "prediction": 1,
        "direction": "up",
        "probability_up": 0.75,
        "model_alias": "production",
        "model_version": "v1",
    }

    logger.log(event)

    assert client.bucket_name == "test-bucket"

    bucket = client.fake_bucket
    blob = bucket.fake_blob

    assert bucket.blob_name is not None
    assert bucket.blob_name.startswith("logs/inference/date=")
    assert bucket.blob_name.endswith(".json")

    uploaded = json.loads(blob.uploaded_data)

    assert uploaded["coin_id"] == "bitcoin"
    assert uploaded["prediction"] == 1
    assert uploaded["model_version"] == "v1"
    assert blob.content_type == "application/json"
