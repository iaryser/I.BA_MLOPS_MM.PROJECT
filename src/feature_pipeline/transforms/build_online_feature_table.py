from pathlib import Path

import pandas as pd

from feature_pipeline.transforms.feature_builder import FeatureBuilder

DATA_PATH = Path("data/staging/market_data.parquet")
ONLINE_STORE_PATH = Path("data/online_store/online_features.parquet")

def build_online_feature_table():
    ONLINE_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    builder = FeatureBuilder(future_horizon=24, include_target=False)
    
    df = pd.read_parquet(DATA_PATH)
    
    df = (
        df.sort_values(["coin_id", "timestamp"])
        .groupby("coin_id")
        .tail(72)
        .reset_index(drop=True)
    )
    
    online_features = builder.build(df=df)
    
    online_features = (
        online_features.sort_values(["coin_id", "timestamp"])
        .groupby("coin_id")
        .tail(1)
        .reset_index(drop=True)
    )
    
    online_features.to_parquet(ONLINE_STORE_PATH, index=False)
    
if __name__ == "__main__":
    build_online_feature_table()
    