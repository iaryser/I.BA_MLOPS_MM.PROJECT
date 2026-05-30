from datetime import UTC, datetime

from inference.inference_logger import InferenceLogger
from inference.model_loader import ModelLoader
from inference.online_feature_loader import OnlineFeatureLoader
from inference.predictor import Predictor
from inference.schemas import PredictionResponse


class PredictionService:
    def __init__(
        self,
        model_loader: ModelLoader,
        feature_loader: OnlineFeatureLoader,
        logger: InferenceLogger,
    ) -> None:
        self.model_loader = model_loader
        self.feature_loader = feature_loader
        self.logger = logger

    def predict(self, coin_id: str) -> PredictionResponse:
        loaded_model = self.model_loader.get_model()
        features = self.feature_loader.load_features(coin_id=coin_id)

        predictor = Predictor(model=loaded_model.model)

        prediction_data = predictor.execute_prediction(features=features)
        context_data = self.feature_loader.load_context(coin_id=coin_id)

        res = PredictionResponse(
            coin_id=coin_id,
            timestamp=context_data.get("timestamp").strftime("%Y-%m-%d %H:%M:%S"),
            prediction=prediction_data.get("y_pred"),
            direction=prediction_data.get("direction"),
            probability_up=prediction_data.get("y_proba"),
            model_alias=loaded_model.alias,
            model_version=loaded_model.version,
        )

        try:
            self.logger.log(
                {
                    "logged_at": datetime.now(UTC).isoformat(),
                    "coin_id": res.coin_id,
                    "prediction_timestamp": res.timestamp,
                    "prediction": res.prediction,
                    "direction": res.direction,
                    "probability_up": res.probability_up,
                    "model_alias": res.model_alias,
                    "model_version": res.model_version,
                }
            )
        except Exception:
            print("Failed to write inference log")

        return res
