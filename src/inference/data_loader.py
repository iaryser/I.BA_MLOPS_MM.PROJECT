from pathlib import Path

import pandas as pd


class BaseDataLoader:
    
    def __init__(self, data_path: Path) -> None:
        self.data_path = data_path
        self.df = pd.read_parquet(data_path)

    def reload(self) -> None:
        self.df = pd.read_parquet(self.data_path)