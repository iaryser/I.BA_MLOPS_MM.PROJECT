import pandas as pd
from xgboost import XGBClassifier


class Predictor:
    def __init__(self, model: XGBClassifier) -> None:
        self.model = model

    def execute_prediction(self, features: pd.DataFrame) -> dict[str, float | int | str]:
        y_proba = float(self.model.predict_proba(features)[0, 1])
        y_pred = int(y_proba >= 0.5)

        return {
            "y_proba": y_proba,
            "y_pred": y_pred,
            "direction": "up" if y_pred == 1 else "down"
        }