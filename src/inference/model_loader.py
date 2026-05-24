import os

import joblib
import wandb
from dotenv import load_dotenv

from inference.schemas import LoadedModel

load_dotenv()


class ModelLoader:
    def __init__(self, artifact_name: str, alias: str, model_name: str) -> None:
        self.wandb_api_key = os.getenv("WANDB_API_KEY")

        if not self.wandb_api_key:
            raise RuntimeError("Wandb API key not set!")

        self.entity = os.getenv("WANDB_ENTITY")

        if not self.entity:
            raise RuntimeError("Wandb entity not set!")

        self.project = os.getenv("WANDB_PROJECT")

        if not self.project:
            raise RuntimeError("Wandb project not set!")

        self.artifact_name = artifact_name
        self.alias = alias
        self.model_name = model_name

        self.model = self._load_model()

    def get_model(self) -> LoadedModel:
        return self.model

    def _load_model(self) -> LoadedModel:
        api = wandb.Api()

        artifact = api.artifact(
            f"{self.entity}/{self.project}/{self.artifact_name}:{self.alias}"
        )

        artifact_dir = artifact.download()

        model_path = f"{artifact_dir}/{self.model_name}.joblib"

        model = joblib.load(model_path)

        return LoadedModel(model=model, version=artifact.version, alias=self.alias)
