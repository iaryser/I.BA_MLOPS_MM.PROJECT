import numpy as np
import pandas as pd


class FeatureBuilder:
    
    def __init__(self, future_horizon: int, include_target: bool) -> None:
        self.future_horizon = future_horizon
        self.include_target = include_target
        
    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["coin_id", "timestamp"]).copy()
        
        raw_cols = ["price", "market_cap", "volume"]

        df["is_missing_raw_data"] = df[raw_cols].isna().any(axis=1)

        df[raw_cols] = (df.groupby("coin_id")[raw_cols].ffill(limit=1))

        df = df.dropna(subset=raw_cols).copy()

        df = df[df["market_cap"] > 0].copy()

        grouped = df.groupby("coin_id")

        #Features
        df["return"] = grouped["price"].pct_change(fill_method=None)
        df["return_6"] = grouped["price"].pct_change(6, fill_method=None)
        df["return_12"] = grouped["price"].pct_change(12, fill_method=None)
        df["return_24"] = grouped["price"].pct_change(24, fill_method=None)

        
        df["ma_deviation_6"] = df["price"] / grouped["price"].transform(
            lambda x: x.rolling(6).mean()
        ) - 1
        df["ma_deviation_12"] = df["price"] / grouped["price"].transform(
            lambda x: x.rolling(12).mean()
        ) - 1
        df["ma_deviation_24"] = df["price"] / grouped["price"].transform(
            lambda x: x.rolling(24).mean()
        ) - 1
        

        df["volatility_6"] = grouped["return"].transform(
            lambda x: x.rolling(6).std()
        )
        df["volatility_12"] = grouped["return"].transform(
            lambda x: x.rolling(12).std()
        )
        df["volatility_24"] = grouped["return"].transform(
            lambda x: x.rolling(24).std()
        )


        df["normalized_momentum_6"] = df["return_6"] / df["volatility_6"]
        df["normalized_momentum_12"] = df["return_12"] / df["volatility_12"]
        df["normalized_momentum_24"] = df["return_24"] / df["volatility_24"]


        df["log_volume"] = np.log1p(df["volume"])
        df["log_volume_change_6"] = grouped["log_volume"].diff(6)
        df["log_volume_change_12"] = grouped["log_volume"].diff(12)
        df["log_volume_change_24"] = grouped["log_volume"].diff(24)
        

        df["volume_to_mcap"] = np.log1p(
            df["volume"] / df["market_cap"].replace(0, np.nan)
        )
        
        if self.include_target:
            
            #Target variable
            future = df[["coin_id", "timestamp", "price"]].copy()

            future["timestamp"] = future["timestamp"] - pd.Timedelta(hours=self.future_horizon)

            future = future.rename(columns={"price": "future_price"})

            df = df.merge(
                future,
                on=["coin_id", "timestamp"],
                how="left",
            )

            df = df.dropna(subset=["future_price"]).copy()

            df["target"] = (df["future_price"] > df["price"]).astype(int)

            df = df.drop(columns="future_price")
            

        #Getting rid of possibly bad values
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna().copy()
        
        df = df.drop(columns=["log_volume", "is_missing_raw_data"])
        
        df = df.reset_index(drop=True).copy()
        return df