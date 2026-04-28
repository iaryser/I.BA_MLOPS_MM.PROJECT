import pandas as pd 

class FeatureBuilder:
        
    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["coin_id", "timestamp"]).copy()

        grouped = df.groupby("coin_id")

        df["return"] = grouped["price"].pct_change()

        #Features
        df["moving_average_5"] = grouped["price"].transform(
            lambda x: x.rolling(5).mean()
        )

        df["volatility_5"] = grouped["return"].transform(
            lambda x: x.rolling(5).std()
        )

        df["momentum_5"] = grouped["price"].transform(
            lambda x: x - x.shift(5)
        )

        df["volume_change"] = grouped["volume"].pct_change()

        df["volume_to_mcap"] = df["volume"] / df["market_cap"]
        
        #Target variable
        df["future_price"] = grouped["price"].shift(-1)
        df["target"] = (df["future_price"] > df["price"]).astype(int)
        
        df = df.drop(columns="future_price")
        
        df = df.dropna().reset_index(drop=True).copy() #remove rows that dont contain all the features
        
        return df