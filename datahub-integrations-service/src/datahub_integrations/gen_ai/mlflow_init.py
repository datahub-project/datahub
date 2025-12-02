import os
from typing import Optional

import mlflow
import mlflow.tracing
from loguru import logger
from mlflow import bedrock

__all__ = ["is_mlflow_enabled", "initialize_mlflow"]

_mlflow_enabled: Optional[bool] = None


def is_mlflow_enabled() -> bool:
    """
    Check if MLflow is enabled.

    Returns False if MLflow hasn't been initialized yet (safe default).
    This allows the function to be called anytime without requiring
    initialize_mlflow() to be called first.
    """
    if _mlflow_enabled is None:
        logger.debug(
            "is_mlflow_enabled() called before initialize_mlflow() - returning False"
        )
        return False
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
        logger.debug(
            f"Initializing MLflow with tracking URI = {os.environ['MLFLOW_TRACKING_URI']}"
        )
        # Enable autologging for all LLM providers
        bedrock.autolog()  # AWS Bedrock
        mlflow.openai.autolog()  # OpenAI direct API calls
        mlflow.gemini.autolog()  # Google Gemini/Vertex AI direct API calls
        mlflow.langchain.autolog()  # LangChain invoke() calls (used by OpenAI/Gemini wrappers)
        mlflow.config.enable_async_logging()
        _mlflow_enabled = True
    else:
        logger.debug("MLflow tracking disabled - no MLFLOW_TRACKING_URI found")
        mlflow.tracing.disable()
        _mlflow_enabled = False
