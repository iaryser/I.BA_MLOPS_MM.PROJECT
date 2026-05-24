from inference.model_loader import ModelLoader
from inference.online_feature_loader import OnlineFeatureLoader
from inference.predictor import Predictor
from inference.schemas import PredictionResponse


class PredictionService:
    def __init__(
        self,
        model_loader: ModelLoader,
        feature_loader: OnlineFeatureLoader,
    ) -> None:
        self.model_loader = model_loader
        self.feature_loader = feature_loader

    def predict(self, coin_id: str) -> PredictionResponse:
        loaded_model = self.model_loader.get_model()
        features = self.feature_loader.load_features(coin_id=coin_id)

        predictor = Predictor(model=loaded_model.model)

        prediction_data = predictor.execute_prediction(features=features)
        context_data = self.feature_loader.load_context(coin_id=coin_id)

        res = PredictionResponse(
            coin_id=coin_id,
            timestamp=context_data.get("timestamp").strftime("%Y-%m-%d %H:%M:%S"),
            prediciton=prediction_data.get("y_pred"),
            direction=prediction_data.get("direction"),
            probability_up=prediction_data.get("y_proba"),
            model_alias=loaded_model.alias,
        )
        return res
