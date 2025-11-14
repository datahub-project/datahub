import os
from typing import Optional

import mlflow
import mlflow.tracing
from loguru import logger
from mlflow import bedrock

__all__ = ["is_mlflow_enabled", "initialize_mlflow"]


class MLflowNotInitializedError(Exception):
    """Raised when MLflow functionality is used before initialization."""

    pass


_mlflow_enabled: Optional[bool] = None


def is_mlflow_enabled() -> bool:
    """
    Check if MLflow is enabled.

    Note: This is a pure query function - it does NOT initialize MLflow.
    Call initialize_mlflow() first if you need MLflow to be set up.

    Raises:
        MLflowNotInitializedError: If MLflow has not been initialized yet.
    """
    if _mlflow_enabled is None:
        raise MLflowNotInitializedError(
            "MLflow has not been initialized. Call initialize_mlflow() first."
        )
    return _mlflow_enabled


def initialize_mlflow() -> None:
    """
    Initialize MLflow based on environment configuration.

    Safe to call multiple times - initialization only happens once.
    """
    global _mlflow_enabled
    if _mlflow_enabled is not None:
        return  # Already initialized

    if "MLFLOW_TRACKING_URI" in os.environ:
        logger.debug("Initializing MLflow with tracking URI")
        bedrock.autolog()
        mlflow.litellm.autolog()
        # mlflow.langchain.autolog()
        mlflow.config.enable_async_logging()
        _mlflow_enabled = True
    else:
        logger.debug("MLflow tracking disabled - no MLFLOW_TRACKING_URI found")
        mlflow.tracing.disable()
        _mlflow_enabled = False
