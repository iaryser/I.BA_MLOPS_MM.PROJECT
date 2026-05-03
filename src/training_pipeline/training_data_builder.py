import pandas as pd


class TrainingDataBuilder:
    
    def filter_df_on_valid_coins(self, feature_df: pd.DataFrame, min_datapoints_per_coin: int) -> pd.DataFrame:
        valid_coins = []
        
        df = feature_df
        
        coin_ids = df.groupby("coin_id")["coin_id"].count()
        
        for coin, count in coin_ids.items():
            if count >= min_datapoints_per_coin:
                valid_coins.append(coin)

        df = df[df["coin_id"].isin(valid_coins)]
        
        return df
    

    def split_time_series_data(
        self,
        feature_df: pd.DataFrame,
        target_var: pd.Series,
        train_size: float = 0.7,
        val_size: float = 0.15,) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series,]:
        
        X = feature_df
        y = target_var
        
        train_end = int(len(X) * train_size)
        val_end = int(len(X) * (train_size + val_size))
        
        X_train = X.iloc[:train_end, :]
        y_train = y.iloc[:train_end]
        
        X_val = X.iloc[train_end:val_end, :]
        y_val = y.iloc[train_end:val_end]
        
        X_test = X.iloc[val_end:, :]
        y_test = y.iloc[val_end:]
        
        return X_train, y_train, X_val, y_val, X_test, y_test