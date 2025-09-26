from datahub_integrations.gen_ai.mlflow_init import initialize_mlflow, is_mlflow_enabled

import os
import warnings

import dotenv

assert dotenv.load_dotenv()
os.environ["DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL"] = "http://localhost:3000"

# Initialize MLflow (this will work with or without MLFLOW_TRACKING_URI)
initialize_mlflow()

# Check if MLflow is enabled and provide appropriate feedback
if is_mlflow_enabled():
    # It's not clear to me why this import is required. Without it, calls
    # to mlflow.evaluate() seem to hang.
    import mlflow.models.evaluation.evaluator_registry as evaluator_registry

    assert evaluator_registry is not None
else:
    warnings.warn(
        "MLflow tracking is disabled - no MLFLOW_TRACKING_URI found in environment. "
        "MLflow features will not be available, but the application will continue to work.",
        stacklevel=2,
    )

AI_EXPERIMENTATION_INITIALIZED = True
