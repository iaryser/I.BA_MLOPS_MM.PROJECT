import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
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
from training_data_builder import TrainingDataBuilder
from xgboost import XGBClassifier

import wandb

load_dotenv()

API_KEY = os.getenv("WANDB_API_KEY")
WANDB_ENTITY = os.getenv("WANDB_ENTITY")
WANDB_PROJECT = os.getenv("WANDB_PROJECT")

if not API_KEY:
    raise RuntimeError("WANDB_API_KEY not set")

if not WANDB_ENTITY: 
    raise RuntimeError("WANDB_ENTITY not set")

if not WANDB_PROJECT: 
    raise RuntimeError("WANDB_PROJECT not set")

CURRENT_MODEL_PATH = Path("models/train/xgboost_model.joblib")
PRODUCTION_MODEL_PATH = Path("models/production/xgboost_model.joblib")

MODEL_ARTIFACT_NAME = "xgboost-direction-model"

BEST_ALIAS = "best-v2"
LATEST_ALIAS = "latest-v2"

FEATURE_DATA = Path("data/aggregated/feature_data.parquet")

FEATURE_COLUMNS = [
    'volume',
    'return',
    'return_6',
    'return_12',
    'return_24',
    'ma_deviation_6',
    'ma_deviation_12',
    'ma_deviation_24',
    'volatility_6',
    'volatility_12',
    'volatility_24',
    'normalized_momentum_6',
    'normalized_momentum_12',
    'normalized_momentum_24',
    'log_volume_change_6',
    'log_volume_change_12',
    'log_volume_change_24',
    'volume_to_mcap'
]


sweep_config = {
    "method": "random",
    "metric": {
        "name": "mean_val_f1_score",
        "goal": "maximize",
    },
    "parameters": {
        "booster": {"value": "gbtree"},
        "max_depth": {"values": [2, 3, 4]},
        "learning_rate": {"values": [0.03, 0.05, 0.1]},
        "subsample": {"values": [0.8, 1.0]},
        "n_estimators": {"values": [100, 200, 400]},
        "min_child_weight": {"values": [1, 3, 5]},
        "colsample_bytree": {"values": [0.8, 1.0]},
        "gamma": {"values": [0, 0.1]},
        "reg_lambda": {"values": [1, 2, 5]},
        "reg_alpha": {"values": [0, 0.01]},
    },
}

def load_wandb_config() -> tuple[str, str, str]:
    api_key = os.getenv("WANDB_API_KEY")
    entity = os.getenv("WANDB_ENTITY")
    project = os.getenv("WANDB_PROJECT")

    if not api_key:
        raise RuntimeError("WANDB_API_KEY not set")
    if not entity:
        raise RuntimeError("WANDB_ENTITY not set")
    if not project:
        raise RuntimeError("WANDB_PROJECT not set")

    return api_key, entity, project


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
    

def train(df: pd.DataFrame):
    with wandb.init() as run:
        config = run.config

        df = df.sort_values("timestamp").copy()

        start = df["timestamp"].min()
        end = df["timestamp"].max()

        train_start = start
        train_end = train_start + relativedelta(months=3)

        fold_metrics = []
        fold = 0

        while True:
            val_start = train_end
            val_end = val_start + relativedelta(months=1)

            if val_end > end:
                break

            train_fold = df[
                (df["timestamp"] >= train_start)
                & (df["timestamp"] < train_end)
            ]

            val_fold = df[
                (df["timestamp"] >= val_start)
                & (df["timestamp"] < val_end)
            ]

            X_train = train_fold[FEATURE_COLUMNS]
            y_train = train_fold["target"]

            X_val = val_fold[FEATURE_COLUMNS]
            y_val = val_fold["target"]

            model = create_model(config=config)

            model.fit(X_train, y_train)

            y_proba = model.predict_proba(X_val)[:, 1]
            
            best_threshold = 0.5
            best_f1 = -1
            best_y_pred = None

            for threshold in [0.45, 0.46, 0.47, 0.48, 0.49, 0.5]:
                candidate_y_pred = (y_proba >= threshold).astype(int)
                candidate_f1 = f1_score(y_val, candidate_y_pred, zero_division=0)

                if candidate_f1 > best_f1:
                    best_f1 = candidate_f1
                    best_threshold = threshold
                    best_y_pred = candidate_y_pred

            metrics = evaluate_model(
                y_true=y_val,
                y_pred=best_y_pred,
                y_proba=y_proba,
            )

            metrics["threshold"] = best_threshold

            fold_metrics.append(metrics)

            train_end += relativedelta(months=1)
            fold += 1

        metrics_df = pd.DataFrame(fold_metrics)

        summary_metrics = {
            "mean_val_accuracy": metrics_df["accuracy"].mean(),
            "mean_val_precision": metrics_df["precision"].mean(),
            "mean_val_recall": metrics_df["recall"].mean(),
            "mean_val_f1_score": metrics_df["f1_score"].mean(),
            "mean_val_roc_auc_score": metrics_df["roc_auc_score"].mean(),
            "mean_val_log_loss": metrics_df["log_loss"].mean(),
            "mean_val_threshold": metrics_df["threshold"].mean(),
            "mean_val_balanced_accuracy": metrics_df["balanced_accuracy"].mean(),
            "mean_val_predicted_positive_rate": metrics_df["predicted_positive_rate"].mean(),
            "mean_val_actual_positive_rate": metrics_df["actual_positive_rate"].mean(),
            "n_folds": len(metrics_df)
        }

        wandb.log(summary_metrics)

def get_best_config_and_threshold(entity: str, project: str, sweep_id: str) -> tuple[dict, float]:
    api = wandb.Api()

    sweep = api.sweep(f"{entity}/{project}/{sweep_id}")
    best_run = sweep.best_run()
    
    best_config = dict(best_run.config)
    best_threshold = best_run.summary.get("mean_val_threshold", 0.5)
    

    return best_config, best_threshold



def load_current_best_f1(run) -> float | None:
    try:
        artifact = run.use_artifact(
            f"{MODEL_ARTIFACT_NAME}:{BEST_ALIAS}",
            type="model",
        )
        return artifact.metadata.get("test_f1_score")
    except Exception:
        return None
    
    
def train_final_model(
    best_config: dict,
    best_threshold: float,
    train_val_df: pd.DataFrame,
    test_df: pd.DataFrame
    ) -> bool:
    with wandb.init(
        config=best_config,
        project=WANDB_PROJECT,
        entity=WANDB_ENTITY
        ) as run:
        
        model = create_model(run.config)
        
        X_train = train_val_df[FEATURE_COLUMNS]
        y_train = train_val_df["target"]
        
        X_test = test_df[FEATURE_COLUMNS]
        y_test = test_df["target"]
        
        model.fit(X_train, y_train)
        
        y_proba = model.predict_proba(X_test)[:, 1]
        y_pred = (y_proba >= best_threshold).astype(int)
        
        test_metrics = evaluate_model(
            y_true=y_test,
            y_pred=y_pred,
            y_proba=y_proba
        )
        
        run.log({
            "test_accuracy": test_metrics["accuracy"],
            "test_precision": test_metrics["precision"],
            "test_recall": test_metrics["recall"],
            "test_f1_score": test_metrics["f1_score"],
            "test_roc_auc_score": test_metrics["roc_auc_score"],
            "test_log_loss": test_metrics["log_loss"],
            "test_balanced_accuracy": test_metrics["balanced_accuracy"],
            "test_predicted_positive_rate": test_metrics["predicted_positive_rate"],
            "test_actual_positive_rate": test_metrics["actual_positive_rate"],
            "decision_threshold": best_threshold,
        })
        
        joblib.dump(model, CURRENT_MODEL_PATH)
        
        artifact = wandb.Artifact(
            name=MODEL_ARTIFACT_NAME,
            type="model",
            metadata={
                "test_f1_score": test_metrics["f1_score"],
                "test_accuracy": test_metrics["accuracy"],
                "decision_threshold": best_threshold,
                "trained_on": "train_val",
                "features": FEATURE_COLUMNS,
                "hyperparameters": best_config,
                "test_balanced_accuracy": test_metrics["balanced_accuracy"],
                "test_precision": test_metrics["precision"],
                "test_recall": test_metrics["recall"],
                "test_roc_auc_score": test_metrics["roc_auc_score"],
                "test_predicted_positive_rate": test_metrics["predicted_positive_rate"],
                "test_actual_positive_rate": test_metrics["actual_positive_rate"]
            },
        )
        
        artifact.add_file(
            local_path=CURRENT_MODEL_PATH,
            name="xgboost_model.joblib",
        )
        
        aliases = [LATEST_ALIAS]
        
        best_f1 = load_current_best_f1(run)
        
        is_best = False
        if best_f1 is None or test_metrics["f1_score"] > best_f1:
            aliases.append(BEST_ALIAS)
            is_best = True
            
            
        run.log_artifact(
            artifact,
            aliases=aliases
        )
        
        return is_best
    

def train_production_model(
    best_config: dict,
    best_threshold: float,
    train_val_df: pd.DataFrame,
    test_df: pd.DataFrame
    ) -> None:
    
    with wandb.init(
            config=best_config,
            project=WANDB_PROJECT,
            entity=WANDB_ENTITY
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
                "decision_threshold": best_threshold,
                "trained_on": "all_available_data",
                "features": FEATURE_COLUMNS,
                "hyperparameters": best_config,
            },
        )
        
        artifact.add_file(
            local_path=PRODUCTION_MODEL_PATH,
            name="xgboost_model.joblib",
        )
            
        run.log_artifact(
            artifact,
            aliases=["production"]
        )
        

def main():
    CURRENT_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    PRODUCTION_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    wandb.login(API_KEY)
    
    sweep_id = wandb.sweep(
        sweep_config,
        project=WANDB_PROJECT,
        entity=WANDB_ENTITY
        )
    
        
    feature_df = pd.read_parquet(FEATURE_DATA)
    
    builder = TrainingDataBuilder()
    
    feature_df = builder.filter_df_on_valid_coins(
        feature_df=feature_df,
        min_datapoints_per_coin=8000)
    
    train_val_df, test_df = builder.split_by_time(feature_df)
    
    def train_wrapper():
        train(train_val_df)
            
    wandb.agent(sweep_id, train_wrapper, count=25)    
    
    best_config, best_threshold = get_best_config_and_threshold(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        sweep_id=sweep_id)
    
    is_best = train_final_model(
        best_config=best_config,
        best_threshold=best_threshold,
        train_val_df=train_val_df,
        test_df=test_df
        )
    
    if is_best:
        train_production_model(
            best_config=best_config,
            best_threshold=best_threshold,
            train_val_df=train_val_df,
            test_df=test_df
        )
        

if __name__ == "__main__":
    main()