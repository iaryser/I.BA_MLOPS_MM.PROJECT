import pandas as pd


class TrainingDataBuilder:
    def filter_df_on_valid_coins(
        self, feature_df: pd.DataFrame, min_datapoints_per_coin: int
    ) -> pd.DataFrame:

        valid_coins = []

        df = feature_df

        coin_ids = df.groupby("coin_id")["coin_id"].count()

        for coin, count in coin_ids.items():
            if count >= min_datapoints_per_coin:
                valid_coins.append(coin)

        df = df[df["coin_id"].isin(valid_coins)]

        return df

    def split_by_time(
        self,
        df: pd.DataFrame,
        train_size: float = 0.85,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:

        df = df.sort_values(["timestamp", "coin_id"]).copy()

        timestamps = df["timestamp"].sort_values().unique()

        train_end = int(len(timestamps) * train_size)

        train_timestamps = timestamps[:train_end]
        test_timestamps = timestamps[train_end:]

        train_df = df[df["timestamp"].isin(train_timestamps)]
        test_df = df[df["timestamp"].isin(test_timestamps)]

        return train_df, test_df
