import click

from feature_pipeline.ingestion.sync_hourly import batch_load_coin_data
from feature_pipeline.transforms.build_online_feature_table import (
    build_online_feature_table,
)
from feature_pipeline.transforms.build_staging_table import build_staging_table
from feature_pipeline.transforms.calculate_features import build_offline_feature_table


@click.command()
@click.option("--n-coins", default=100, show_default=True, type=int, help="Number of Coins to extract Data from")
@click.option("--currency", default="chf", show_default=True, type=str, help="Currency of Coin-Data")

def run_batch_pipeline(n_coins: int, currency: str) -> None:
    print(f"Step 1/4 batch donwloading raw CoinGecko Data for {n_coins} coins...")
    batch_load_coin_data(n_coins=n_coins, currency=currency)
    
    print("Step 2/4 Building staging market table...")
    build_staging_table()
    
    print("Step 3/4 building offline feature table...")
    build_offline_feature_table()
    
    print("Step 4/4 Using latest market data to build online feature table...")
    build_online_feature_table()
    
    print("Batch pipeline completed successfully")
    
if __name__ == "__main__":
    run_batch_pipeline()