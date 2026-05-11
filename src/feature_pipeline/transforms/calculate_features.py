from pathlib import Path

import pandas as pd

from feature_pipeline.transforms.feature_builder import FeatureBuilder

DATA_PATH = Path("data/staging/market_data.parquet")
OUTPUT_PATH = Path("data/aggregated/feature_data.parquet")

HORIZON = 24

def build_offline_feature_table() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_parquet(DATA_PATH)
    
    builder = FeatureBuilder(future_horizon=HORIZON, include_target=True)
    feature_df = builder.build(df)
    
    feature_df.to_parquet(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    build_offline_feature_table()