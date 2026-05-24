from pathlib import Path

import pytest

from inference import model_loader
from inference.model_loader import ModelLoader


class FakeArtifact:
    version = "v3"

    def __init__(self, download_dir: Path) -> None:
        self.download_dir = download_dir

    def download(self) -> str:
        return str(self.download_dir)


class FakeWandbApi:
    def __init__(self, download_dir: Path) -> None:
        self.download_dir = download_dir
        self.requested_artifact_name: str | None = None

    def artifact(self, artifact_name: str) -> FakeArtifact:
        self.requested_artifact_name = artifact_name
        return FakeArtifact(self.download_dir)


class FakeModel:
    pass


def test_model_loader_loads_model_from_wandb_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_api = FakeWandbApi(download_dir=tmp_path)
    fake_model = FakeModel()

    monkeypatch.setenv("WANDB_API_KEY", "fake-api-key")
    monkeypatch.setenv("WANDB_ENTITY", "fake-entity")
    monkeypatch.setenv("WANDB_PROJECT", "fake-project")

    monkeypatch.setattr(model_loader.wandb, "Api", lambda: fake_api)
    monkeypatch.setattr(model_loader.joblib, "load", lambda path: fake_model)

    loader = ModelLoader(
        artifact_name="xgboost-direction-model",
        alias="production",
        model_name="xgboost_model",
    )

    loaded_model = loader.get_model()

    assert fake_api.requested_artifact_name == (
        "fake-entity/fake-project/xgboost-direction-model:production"
    )
    assert loaded_model.model is fake_model
    assert loaded_model.version == "v3"
    assert loaded_model.alias == "production"


def test_model_loader_loads_expected_joblib_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_api = FakeWandbApi(download_dir=tmp_path)
    captured_paths: list[str] = []

    def fake_joblib_load(path: str) -> FakeModel:
        captured_paths.append(path)
        return FakeModel()

    monkeypatch.setenv("WANDB_API_KEY", "fake-api-key")
    monkeypatch.setenv("WANDB_ENTITY", "fake-entity")
    monkeypatch.setenv("WANDB_PROJECT", "fake-project")

    monkeypatch.setattr(model_loader.wandb, "Api", lambda: fake_api)
    monkeypatch.setattr(model_loader.joblib, "load", fake_joblib_load)

    ModelLoader(
        artifact_name="xgboost-direction-model",
        alias="production",
        model_name="xgboost_model",
    )

    assert captured_paths == [f"{tmp_path}/xgboost_model.joblib"]


def test_model_loader_raises_error_when_wandb_api_key_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WANDB_API_KEY", raising=False)
    monkeypatch.setenv("WANDB_ENTITY", "fake-entity")
    monkeypatch.setenv("WANDB_PROJECT", "fake-project")

    with pytest.raises(RuntimeError, match="Wandb API key not set"):
        ModelLoader(
            artifact_name="xgboost-direction-model",
            alias="production",
            model_name="xgboost_model",
        )


def test_model_loader_raises_error_when_wandb_entity_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WANDB_API_KEY", "fake-api-key")
    monkeypatch.delenv("WANDB_ENTITY", raising=False)
    monkeypatch.setenv("WANDB_PROJECT", "fake-project")

    with pytest.raises(RuntimeError, match="Wandb entity not set"):
        ModelLoader(
            artifact_name="xgboost-direction-model",
            alias="production",
            model_name="xgboost_model",
        )


def test_model_loader_raises_error_when_wandb_project_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WANDB_API_KEY", "fake-api-key")
    monkeypatch.setenv("WANDB_ENTITY", "fake-entity")
    monkeypatch.delenv("WANDB_PROJECT", raising=False)

    with pytest.raises(RuntimeError, match="Wandb project not set"):
        ModelLoader(
            artifact_name="xgboost-direction-model",
            alias="production",
            model_name="xgboost_model",
        )
