import pandas as pd

from training_pipeline.training_data_builder import TrainingDataBuilder


def make_feature_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "coin_id": [
                "bitcoin",
                "bitcoin",
                "bitcoin",
                "ethereum",
                "ethereum",
                "solana",
            ],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                    "2026-01-01 02:00:00+00:00",
                    "2026-01-01 00:00:00+00:00",
                    "2026-01-01 01:00:00+00:00",
                    "2026-01-01 00:00:00+00:00",
                ]
            ),
            "return": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
            "target": [1, 0, 1, 0, 1, 0],
        }
    )


def test_filter_df_on_valid_coins_keeps_coins_with_enough_rows() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    result = builder.filter_df_on_valid_coins(
        feature_df=df,
        min_datapoints_per_coin=2,
    )

    assert set(result["coin_id"]) == {"bitcoin", "ethereum"}


def test_filter_df_on_valid_coins_removes_coins_with_too_few_rows() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    result = builder.filter_df_on_valid_coins(
        feature_df=df,
        min_datapoints_per_coin=3,
    )

    assert set(result["coin_id"]) == {"bitcoin"}
    assert len(result) == 3


def test_filter_df_on_valid_coins_keeps_all_rows_for_valid_coins() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    result = builder.filter_df_on_valid_coins(
        feature_df=df,
        min_datapoints_per_coin=2,
    )

    bitcoin_rows = result[result["coin_id"] == "bitcoin"]
    ethereum_rows = result[result["coin_id"] == "ethereum"]

    assert len(bitcoin_rows) == 3
    assert len(ethereum_rows) == 2


def test_filter_df_on_valid_coins_returns_empty_df_when_no_coin_is_valid() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    result = builder.filter_df_on_valid_coins(
        feature_df=df,
        min_datapoints_per_coin=10,
    )

    assert result.empty


def test_split_by_time_splits_on_unique_timestamps_not_rows() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=0.5)

    assert set(train_df["timestamp"]) == {pd.Timestamp("2026-01-01 00:00:00+00:00")}
    assert set(test_df["timestamp"]) == {
        pd.Timestamp("2026-01-01 01:00:00+00:00"),
        pd.Timestamp("2026-01-01 02:00:00+00:00"),
    }


def test_split_by_time_keeps_train_before_test() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=0.67)

    assert train_df["timestamp"].max() < test_df["timestamp"].min()


def test_split_by_time_does_not_split_same_timestamp_across_train_and_test() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=0.5)

    train_timestamps = set(train_df["timestamp"])
    test_timestamps = set(test_df["timestamp"])

    assert train_timestamps.isdisjoint(test_timestamps)


def test_split_by_time_sorts_output_by_timestamp_and_coin_id() -> None:
    df = make_feature_data().sample(frac=1.0, random_state=42).reset_index(drop=True)
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=0.67)

    combined = pd.concat([train_df, test_df], ignore_index=True)
    expected = combined.sort_values(["timestamp", "coin_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(combined.reset_index(drop=True), expected)


def test_split_by_time_with_train_size_one_puts_all_rows_in_train() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=1.0)

    assert len(train_df) == len(df)
    assert test_df.empty


def test_split_by_time_with_train_size_zero_puts_all_rows_in_test() -> None:
    df = make_feature_data()
    builder = TrainingDataBuilder()

    train_df, test_df = builder.split_by_time(df, train_size=0.0)

    assert train_df.empty
    assert len(test_df) == len(df)
