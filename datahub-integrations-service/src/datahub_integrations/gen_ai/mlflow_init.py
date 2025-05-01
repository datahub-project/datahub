import os

import mlflow
import mlflow.tracing
from mlflow import bedrock

__all__ = ["MLFLOW_INITIALIZED", "MLFLOW_ENABLED"]

MLFLOW_INITIALIZED = True
if "MLFLOW_TRACKING_URI" in os.environ:
    bedrock.autolog()

    MLFLOW_ENABLED = True
else:
    mlflow.tracing.disable()
    MLFLOW_ENABLED = False
