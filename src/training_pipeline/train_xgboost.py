import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import wandb
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from numpy.typing import NDArray
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from xgboost import XGBClassifier

from training_pipeline.training_data_builder import TrainingDataBuilder

# ---------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------

load_dotenv()

API_KEY = os.getenv("WANDB_API_KEY")
WANDB_ENTITY = os.getenv("WANDB_ENTITY")
WANDB_PROJECT = os.getenv("WANDB_PROJECT")


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

FEATURE_DATA = Path("data/aggregated/feature_data.parquet")
CURRENT_MODEL_PATH = Path("models/train/xgboost_model.joblib")
PRODUCTION_MODEL_PATH = Path("models/production/xgboost_model.joblib")


# ---------------------------------------------------------------------
# W&B artifact configuration
# ---------------------------------------------------------------------

MODEL_ARTIFACT_NAME = "xgboost-direction-model"
BEST_ALIAS = "best-v3"
LATEST_ALIAS = "latest-v3"
PRODUCTION_ALIAS = "production"


# ---------------------------------------------------------------------
# Modeling configuration
# ---------------------------------------------------------------------

PREDICTION_THRESHOLD = 0.5
MIN_DATAPOINTS_PER_COIN = 8000
SWEEP_RUN_COUNT = 50
TRAIN_WINDOW_MONTHS = 3
VAL_WINDOW_MONTHS = 1


# ---------------------------------------------------------------------
# Feature Columns
# ---------------------------------------------------------------------

FEATURE_COLUMNS = [
    "return",
    "return_6",
    "return_12",
    "return_24",
    "ma_deviation_6",
    "ma_deviation_12",
    "ma_deviation_24",
    "volatility_6",
    "volatility_12",
    "volatility_24",
    "normalized_momentum_6",
    "normalized_momentum_12",
    "normalized_momentum_24",
    "log_volume_change_6",
    "log_volume_change_12",
    "log_volume_change_24",
    "volume_to_mcap",
]


# ---------------------------------------------------------------------
# Sweep Confiugration
# ---------------------------------------------------------------------

sweep_config = {
    "method": "random",
    "metric": {
        "name": "mean_val_balanced_accuracy",
        "goal": "maximize",
    },
    "parameters": {
        "booster": {"value": "gbtree"},
        "max_depth": {"values": [1, 2, 3, 4, 5]},
        "learning_rate": {"values": [0.01, 0.02, 0.03, 0.05, 0.1]},
        "n_estimators": {"values": [100, 200, 400, 600]},
        "min_child_weight": {"values": [1, 3, 5, 10]},
        "subsample": {"values": [0.6, 0.8, 1.0]},
        "colsample_bytree": {"values": [0.6, 0.8, 1.0]},
        "gamma": {"values": [0, 0.1, 0.5, 1]},
        "reg_lambda": {"values": [1, 2, 5, 10]},
        "reg_alpha": {"values": [0, 0.01, 0.1, 1]},
    },
}


def evaluate_model(
    y_true: pd.Series,
    y_pred: NDArray[np.integer],
    y_proba: NDArray[np.floating],
) -> dict[str, float]:
    metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1_score": f1_score(y_true, y_pred, zero_division=0),
        "roc_auc_score": roc_auc_score(y_true, y_proba),
        "log_loss": log_loss(y_true, y_proba),
        "predicted_positive_rate": y_pred.mean(),
        "actual_positive_rate": y_true.mean(),
    }

    return metrics


def create_model(config) -> XGBClassifier:

    model = XGBClassifier(
        booster=config.booster,
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        subsample=config.subsample,
        n_estimators=config.n_estimators,
        min_child_weight=config.min_child_weight,
        colsample_bytree=config.colsample_bytree,
        gamma=config.gamma,
        reg_lambda=config.reg_lambda,
        reg_alpha=config.reg_alpha,
        eval_metric="logloss",
        random_state=42,
    )

    return model


def train(df: pd.DataFrame) -> None:
    with wandb.init() as run:
        config = run.config

        df = df.sort_values("timestamp").copy()

        start = df["timestamp"].min()
        end = df["timestamp"].max()

        train_start = start
        train_end = train_start + relativedelta(months=TRAIN_WINDOW_MONTHS)

        fold_metrics = []

        while True:
            val_start = train_end
            val_end = val_start + relativedelta(months=VAL_WINDOW_MONTHS)

            if val_end > end:
                break

            train_fold = df[
                (df["timestamp"] >= train_start) & (df["timestamp"] < train_end)
            ]

            val_fold = df[(df["timestamp"] >= val_start) & (df["timestamp"] < val_end)]

            X_train = train_fold[FEATURE_COLUMNS]
            y_train = train_fold["target"]

            X_val = val_fold[FEATURE_COLUMNS]
            y_val = val_fold["target"]

            model = create_model(config=config)

            model.fit(X_train, y_train)

            y_proba = model.predict_proba(X_val)[:, 1]
            y_pred = (y_proba >= PREDICTION_THRESHOLD).astype(int)

            metrics = evaluate_model(
                y_true=y_val,
                y_pred=y_pred,
                y_proba=y_proba,
            )

            fold_metrics.append(metrics)

            train_end += relativedelta(months=1)

        metrics_df = pd.DataFrame(fold_metrics)

        summary_metrics = {
            "mean_val_accuracy": metrics_df["accuracy"].mean(),
            "mean_val_precision": metrics_df["precision"].mean(),
            "mean_val_recall": metrics_df["recall"].mean(),
            "mean_val_f1_score": metrics_df["f1_score"].mean(),
            "mean_val_roc_auc_score": metrics_df["roc_auc_score"].mean(),
            "mean_val_log_loss": metrics_df["log_loss"].mean(),
            "mean_val_balanced_accuracy": metrics_df["balanced_accuracy"].mean(),
            "mean_val_predicted_positive_rate": metrics_df[
                "predicted_positive_rate"
            ].mean(),
            "mean_val_actual_positive_rate": metrics_df["actual_positive_rate"].mean(),
            "n_folds": len(metrics_df),
        }

        wandb.log(summary_metrics)


def get_best_config(entity: str, project: str, sweep_id: str) -> dict:
    api = wandb.Api()

    sweep = api.sweep(f"{entity}/{project}/{sweep_id}")
    best_run = sweep.best_run()

    best_config = dict(best_run.config)

    return best_config


def load_current_best_metrics(run) -> dict | None:
    try:
        artifact = run.use_artifact(
            f"{MODEL_ARTIFACT_NAME}:{BEST_ALIAS}",
            type="model",
        )
        return dict(artifact.metadata)
    except Exception:
        return None


def is_better_than_current(
    test_metrics: dict[str, float], current_metrics: dict | None
) -> bool:
    if current_metrics is None:
        return True

    return (
        test_metrics["balanced_accuracy"]
        > current_metrics.get("test_balanced_accuracy", 0)
        and test_metrics["roc_auc_score"]
        >= current_metrics.get("test_roc_auc_score", 0)
        and 0.3 <= test_metrics["predicted_positive_rate"] <= 0.8
    )


def train_final_model(
    best_config: dict, train_val_df: pd.DataFrame, test_df: pd.DataFrame
) -> tuple[bool, dict[str, float]]:
    with wandb.init(
        config=best_config, project=WANDB_PROJECT, entity=WANDB_ENTITY
    ) as run:
        model = create_model(run.config)

        X_train = train_val_df[FEATURE_COLUMNS]
        y_train = train_val_df["target"]

        X_test = test_df[FEATURE_COLUMNS]
        y_test = test_df["target"]

        model.fit(X_train, y_train)

        y_proba = model.predict_proba(X_test)[:, 1]
        y_pred = (y_proba >= PREDICTION_THRESHOLD).astype(int)

        test_metrics = evaluate_model(y_true=y_test, y_pred=y_pred, y_proba=y_proba)

        run.log(
            {
                "test_accuracy": test_metrics["accuracy"],
                "test_precision": test_metrics["precision"],
                "test_recall": test_metrics["recall"],
                "test_f1_score": test_metrics["f1_score"],
                "test_roc_auc_score": test_metrics["roc_auc_score"],
                "test_log_loss": test_metrics["log_loss"],
                "test_balanced_accuracy": test_metrics["balanced_accuracy"],
                "test_predicted_positive_rate": test_metrics["predicted_positive_rate"],
                "test_actual_positive_rate": test_metrics["actual_positive_rate"],
            }
        )

        joblib.dump(model, CURRENT_MODEL_PATH)

        artifact = wandb.Artifact(
            name=MODEL_ARTIFACT_NAME,
            type="model",
            metadata={
                "test_f1_score": test_metrics["f1_score"],
                "test_accuracy": test_metrics["accuracy"],
                "trained_on": "train_val",
                "features": FEATURE_COLUMNS,
                "hyperparameters": best_config,
                "test_balanced_accuracy": test_metrics["balanced_accuracy"],
                "test_precision": test_metrics["precision"],
                "test_recall": test_metrics["recall"],
                "test_roc_auc_score": test_metrics["roc_auc_score"],
                "test_predicted_positive_rate": test_metrics["predicted_positive_rate"],
                "test_actual_positive_rate": test_metrics["actual_positive_rate"],
            },
        )

        artifact.add_file(
            local_path=CURRENT_MODEL_PATH,
            name="xgboost_model.joblib",
        )

        aliases = [LATEST_ALIAS]

        best_metrics = load_current_best_metrics(run)

        better_than_current = is_better_than_current(test_metrics, best_metrics)

        is_best = False
        if better_than_current:
            aliases.append(BEST_ALIAS)
            is_best = True

        run.log_artifact(artifact, aliases=aliases)

        return is_best, test_metrics


def train_production_model(
    best_config: dict,
    train_val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    test_metrics: dict[str, float],
) -> None:

    with wandb.init(
        config=best_config, project=WANDB_PROJECT, entity=WANDB_ENTITY
    ) as run:
        all_data = pd.concat([train_val_df, test_df])

        X_train = all_data[FEATURE_COLUMNS]
        y_train = all_data["target"]

        model = create_model(run.config)

        model.fit(X_train, y_train)

        joblib.dump(model, PRODUCTION_MODEL_PATH)

        artifact = wandb.Artifact(
            name=MODEL_ARTIFACT_NAME,
            type="model",
            metadata={
                "trained_on": "all_available_data",
                "features": FEATURE_COLUMNS,
                "hyperparameters": best_config,
                "validated_test_f1_score": test_metrics["f1_score"],
                "validated_test_accuracy": test_metrics["accuracy"],
                "validated_test_balanced_accuracy": test_metrics["balanced_accuracy"],
                "validated_test_precision": test_metrics["precision"],
                "validated_test_recall": test_metrics["recall"],
                "validated_test_roc_auc_score": test_metrics["roc_auc_score"],
                "validated_test_predicted_positive_rate": test_metrics[
                    "predicted_positive_rate"
                ],
                "validated_test_actual_positive_rate": test_metrics[
                    "actual_positive_rate"
                ],
            },
        )

        artifact.add_file(
            local_path=PRODUCTION_MODEL_PATH,
            name="xgboost_model.joblib",
        )

        run.log_artifact(artifact, aliases=[PRODUCTION_ALIAS])


def main():
    CURRENT_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    PRODUCTION_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    wandb.login(API_KEY)

    sweep_id = wandb.sweep(sweep_config, project=WANDB_PROJECT, entity=WANDB_ENTITY)

    feature_df = pd.read_parquet(FEATURE_DATA)

    builder = TrainingDataBuilder()

    feature_df = builder.filter_df_on_valid_coins(
        feature_df=feature_df, min_datapoints_per_coin=MIN_DATAPOINTS_PER_COIN
    )

    train_val_df, test_df = builder.split_by_time(feature_df)

    def train_wrapper():
        train(train_val_df)

    wandb.agent(sweep_id, train_wrapper, count=SWEEP_RUN_COUNT)

    best_config = get_best_config(
        entity=WANDB_ENTITY, project=WANDB_PROJECT, sweep_id=sweep_id
    )

    is_best, test_metrics = train_final_model(
        best_config=best_config, train_val_df=train_val_df, test_df=test_df
    )

    if is_best:
        train_production_model(
            best_config=best_config,
            train_val_df=train_val_df,
            test_df=test_df,
            test_metrics=test_metrics,
        )


if __name__ == "__main__":
    main()
