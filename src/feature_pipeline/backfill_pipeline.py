import click

from feature_pipeline.ingestion.backfill import backfill_coin_data
from feature_pipeline.transforms.build_online_feature_table import (
    build_online_feature_table,
)
from feature_pipeline.transforms.build_staging_table import build_staging_table
from feature_pipeline.transforms.calculate_features import build_offline_feature_table


@click.command()
@click.option(
    "--n-coins",
    default=100,
    show_default=True,
    type=int,
    help="Number of Coins to extract Data from",
)
@click.option(
    "--currency",
    default="chf",
    show_default=True,
    type=str,
    help="Currency of Coin-Data",
)
@click.option(
    "--n-days",
    default=365,
    show_default=True,
    type=int,
    help="Number of days to fetch from API",
)
def run_backfill_pipeline(n_coins: int, currency: str, n_days: int) -> None:
    print(f"Step 1/4 backfilling raw CoinGecko Data for {n_coins} coins")
    backfill_coin_data(n_coins=n_coins, currency=currency, n_days=n_days)

    print("Step 2/4 Building staging market table")
    build_staging_table(source="backfill")

    print("Step 3/4 building offline feature table")
    build_offline_feature_table()

    print("Step 4/4 Using latest market data to build online feature table")
    build_online_feature_table()

    print("Backfill pipeline completed successfully")


if __name__ == "__main__":
    run_backfill_pipeline()
