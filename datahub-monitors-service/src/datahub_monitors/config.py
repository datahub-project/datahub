import os


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


EXECUTOR_MODE = os.getenv("DATAHUB_EXECUTOR_MODE", "coordinator")

MONITORS_ENABLED = string_to_bool(os.getenv("MONITORS_ENABLED", "True"))
INGESTION_ENABLED = string_to_bool(os.getenv("INGESTION_ENABLED", "True"))
EMBEDDED_WORKER_ENABLED = string_to_bool(os.getenv("EMBEDDED_WORKER_ENABLED", "True"))
EMBEDDED_WORKER_ID = os.getenv("EMBEDDED_WORKER_ID", "default")

EXECUTOR_ID = os.environ.get("EXECUTOR_ID", "default")

DATAHUB_SERVER = (
    f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://"
    f"{os.environ.get('DATAHUB_GMS_HOST', 'localhost')}:{os.environ.get('DATAHUB_GMS_PORT', 8080)}"
)

ACTIONS_PIPELINE_CONFIG_PATH = os.environ.get("ACTIONS_PIPELINE_CONFIG_PATH", "/etc/datahub/actions-pipeline/config.yaml")
