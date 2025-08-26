from datahub_integrations.gen_ai.mlflow_init import MLFLOW_ENABLED

import os

import dotenv

# It's not clear to me why this import is required. Without it, calls
# to mlflow.evaluate() seem to hang.
import mlflow.models.evaluation.evaluator_registry as evaluator_registry

assert dotenv.load_dotenv()
os.environ["DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL"] = "http://localhost:3000"

assert evaluator_registry is not None

assert MLFLOW_ENABLED, "mlflow tracking should be enabled"

AI_EXPERIMENTATION_INITIALIZED = True
