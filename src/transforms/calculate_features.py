import pandas as pd 
from pathlib import Path
from feature_builder import FeatureBuilder

DATA_PATH = Path("data/staging/market_data.parquet")
OUTPUT_PATH = Path("data/aggregated/feature_data.parquet")
    
    
def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_parquet(DATA_PATH)
    
    builder = FeatureBuilder()
    feature_df = builder.build(df)
    
    feature_df.to_parquet(OUTPUT_PATH, index=False)

        
if __name__ == "__main__":
    main()