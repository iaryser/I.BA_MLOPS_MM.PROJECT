import os
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from training_data_builder import TrainingDataBuilder
from wandb.errors import CommError

import wandb

load_dotenv()

if not os.getenv("WANDB_API_KEY"):
    raise RuntimeError("WANDB_API_KEY is missing. Add it to your .env file.")


PROJECT_NAME = "crypto-direction-prediction"
FEATURE_PATH = Path("data/aggregated/feature_data.parquet")
MODEL_PATH = Path("models/logistic_regression.joblib")
MODEL_ARTIFACT_NAME = "logistic-regression-model"

FEATURE_COLS = [
    "return",
    "ma_deviation_5",
    "volatility_5",
    "momentum_5",
    "volume_change",
    "volume_to_mcap",
]


def load_training_data() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    builder = TrainingDataBuilder()

    df = pd.read_parquet(FEATURE_PATH)
    filtered_df = builder.filter_df_on_valid_coins(df, 8700)
    filtered_df = filtered_df.sort_values(["timestamp", "coin_id"]).reset_index(drop=True)

    X = filtered_df[FEATURE_COLS]
    y = filtered_df["target"]

    return builder.split_time_series_data(
        feature_df=X,
        target_var=y,
        train_size=0.7,
        val_size=0.15,
    )


def build_model() -> Pipeline:
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(solver="saga", max_iter=1000)),
        ]
    )


def evaluate_model(model: Pipeline, X_val: pd.DataFrame, y_val: pd.Series) -> dict[str, float]:
    y_pred = model.predict(X_val)

    return {
        "val_accuracy": accuracy_score(y_val, y_pred),
        "val_precision": precision_score(y_val, y_pred, zero_division=0),
        "val_recall": recall_score(y_val, y_pred, zero_division=0),
        "val_f1": f1_score(y_val, y_pred, zero_division=0),
    }


def get_artifact_aliases(run: wandb.sdk.wandb_run.Run, current_f1: float) -> list[str]:
    aliases = ["latest"]

    try:
        best_artifact = run.use_artifact(
            f"{MODEL_ARTIFACT_NAME}:best",
            type="model"
        )
        best_f1 = best_artifact.metadata["metrics"]["val_f1"]

        if current_f1 > best_f1:
            aliases.append("best")

    except CommError:
        aliases.append("best") #in case no model exists yet

    return aliases


def log_model_artifact(
    run: wandb.sdk.wandb_run.Run,
    model: Pipeline,
    metrics: dict[str, float],
) -> None:
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    artifact = wandb.Artifact(
        name=MODEL_ARTIFACT_NAME,
        type="model",
        metadata={
            "metrics": metrics,
            "model_type": "logistic_regression",
            "features": FEATURE_COLS,
        },
    )

    artifact.add_file(
        local_path=str(MODEL_PATH),
        name="logistic_regression.joblib",
    )

    aliases = get_artifact_aliases(run, metrics["val_f1"])
    run.log_artifact(artifact, aliases=aliases)


def main() -> None:
    load_dotenv()

    if not os.getenv("WANDB_API_KEY"):
        raise RuntimeError(...)
    
    X_train, y_train, X_val, y_val, X_test, y_test = load_training_data()

    run = wandb.init(
        project=PROJECT_NAME,
        job_type="train",
        config={
            "model_type": "logistic_regression",
            "train_size": 0.7,
            "validation_size": 0.15,
            "test_size": 0.15,
            "features": FEATURE_COLS,
            "horizon_hours": 1,
        },
    )

    model = build_model()
    model.fit(X_train, y_train)

    metrics = evaluate_model(model, X_val, y_val)

    run.config.update(
        {
            "n_rows_train": len(X_train),
            "n_rows_val": len(X_val),
            "n_rows_test": len(X_test),
        }
    )

    run.log(metrics)
    log_model_artifact(run, model, metrics)

    run.finish()


if __name__ == "__main__":
    main()