from pathlib import Path

import pandas as pd

from inference.data_loader import BaseDataLoader
from inference.schemas import TopCoin


class OnlineFeatureLoader(BaseDataLoader):
    
    def __init__(self, data_path: Path) -> None:
        super().__init__(data_path)
        self._non_feature_columns = [
            "timestamp",
            "coin_id",
            "price",
            "market_cap",
            "volume",
        ]
        

    def get_available_coins(self) -> list[str]:
        self.reload()
        return sorted(self.df["coin_id"].unique().tolist())

    def load_features(self, coin_id: str) -> pd.DataFrame:
        self.reload()
        return (
            self.df[self.df["coin_id"] == coin_id]
            .drop(self._non_feature_columns, axis=1)
        )
        
    def load_context(self, coin_id: str) -> dict:
        row = self.df[self.df["coin_id"] == coin_id].iloc[0]
        return row.to_dict()

    def load_top5_coins(self) -> list[TopCoin]:
        self.reload()
        top_df = (
            self.df
            .sort_values("volume", ascending=False)
            .head(5)
        )

        return [
            TopCoin(
                coin_id=row["coin_id"],
                volume=float(row["volume"]),
            )
            for _, row in top_df.iterrows()
        ]