from model_loader import ModelLoader
from online_feature_loader import OnlineFeatureLoader
from predictor import Predictor

from schemas import *

class PredictionService:
    def __init__(
        self,
        model_loader: ModelLoader,
        feature_loader: OnlineFeatureLoader,
    ) -> None:
        self.model_loader = model_loader
        self.feature_loader = feature_loader

    def predict(self, coin_id: str) -> PredictionResponse:
        loaded_model = self.model_loader.load_model()
        features = self.feature_loader.load_online_features(coin_id=coin_id)
        
        predictor = Predictor(
            model=loaded_model.model,
            threshold=loaded_model.threshold
        )
        
        prediction_data = predictor.execute_prediction(features=features)
        context_data = self.feature_loader.load_online_context(coin_id=coin_id)
        
        res = PredictionResponse(
            coin_id=coin_id,
            timestamp=context_data.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            prediciton=prediction_data.get("y_pred"),
            direction=prediction_data.get("direction"),
            probability_up=prediction_data.get("y_proba"),
            model_version=loaded_model.version
        )
        return res