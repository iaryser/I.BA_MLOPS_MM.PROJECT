from datetime import timedelta
from pathlib import Path

from inference.feature_loader import BaseFeatureLoader


class OfflineFeatureLoader(BaseFeatureLoader):
    
    def __init__(self, data_path: Path) -> None:
        super().__init__(data_path)
        self._context_columns = [
            "timestamp",
            "coin_id",
            "price",
            "market_cap",
            "volume",
        ]
        

    def load_coin_context_data(self, coin_id: str, n_days: int) -> list[dict]:
        hours = n_days * 24

        df = self.df[self._context_columns]
        coin_df = df[df["coin_id"] == coin_id].copy().sort_values("timestamp")

        last_date = coin_df.iloc[-1]["timestamp"]
        date_range = [last_date - timedelta(hours=i) for i in range(hours)]

        coin_df = coin_df[coin_df["timestamp"].isin(date_range)]
        coin_df["timestamp"] = coin_df["timestamp"].astype(str)

        return coin_df.to_dict(orient="records")