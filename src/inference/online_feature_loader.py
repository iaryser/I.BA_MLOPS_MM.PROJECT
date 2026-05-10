import pandas as pd


class OnlineFeatureLoader:
    
    def __init__(self, data_path: str) -> None:
        self.data_path = data_path
        
        self._non_feature_columns = ["timestamp", "coin_id", "price", "market_cap", "volume"]
        
        
    def get_available_coins(self) -> list[str]:
        coins = pd.read_parquet(self.data_path)["coin_id"].unique()
        
        return list(coins)
        
    def load_online_features(self, coin_id) -> pd.DataFrame:
        df = pd.read_parquet(self.data_path)
        
        df = df[df["coin_id"] == coin_id].drop(self._non_feature_columns, axis=1)
        
        return df
    
    def load_online_context(self, coin_id) -> pd.DataFrame:
        df = pd.read_parquet(self.data_path)[self._non_feature_columns]
        
        df = df[df["coin_id"] == coin_id].iloc[0]
        
        return df