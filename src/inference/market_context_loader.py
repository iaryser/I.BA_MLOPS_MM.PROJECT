from datetime import timedelta
from pathlib import Path

from inference.data_loader import BaseDataLoader


class MarketContextLoader(BaseDataLoader):
    def __init__(self, data_path: Path) -> None:
        super().__init__(data_path)

    def load_coin_context_data(self, coin_id: str, n_days: int) -> list[dict]:

        hours = n_days * 24

        coin_df = self.df[self.df["coin_id"] == coin_id].copy().sort_values("timestamp")

        last_date = coin_df.iloc[-1]["timestamp"]
        date_range = [last_date - timedelta(hours=i) for i in range(hours)]

        coin_df = coin_df[coin_df["timestamp"].isin(date_range)]
        coin_df["timestamp"] = coin_df["timestamp"].astype(str)

        return coin_df.to_dict(orient="records")
