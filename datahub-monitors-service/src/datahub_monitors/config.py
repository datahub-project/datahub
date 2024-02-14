import os


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


EXECUTOR_MODE = os.getenv("DATAHUB_EXECUTOR_MODE", "coordinator")

MONITORS_ENABLED = string_to_bool(os.getenv("DATAHUB_EXECUTOR_MONITORS_ENABLED", "True"))
INGESTION_ENABLED = string_to_bool(os.getenv("DATAHUB_EXECUTOR_INGESTION_ENABLED", "True"))
EMBEDDED_WORKER_ENABLED = string_to_bool(os.getenv("DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED", "True"))
EMBEDDED_WORKER_ID = os.getenv("DATAHUB_EXECUTOR_EMBEDDED_WORKER_ID", "default")

EXECUTOR_ID = os.environ.get("DATAHUB_EXECUTOR_WORKER_ID", "default")

DATAHUB_SERVER = (
    f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://"
    f"{os.environ.get('DATAHUB_GMS_HOST', 'localhost')}:{os.environ.get('DATAHUB_GMS_PORT', 8080)}"
)

ACTIONS_PIPELINE_CONFIG_PATH = os.environ.get("DATAHUB_EXECUTOR_ACTIONS_CONFIG_PATH", "/etc/datahub/actions-pipeline/config.yaml")
ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS = int(os.environ.get("DATAHUB_EXECUTOR_ACTIONS_MAX_WORKERS", "4"))
ACTIONS_PIPELINE_SIGNAL_POLL_INTERVAL = int(os.environ.get("DATAHUB_EXECUTOR_ACTIONS_SIGNAL_POLL_INTERVAL", "2"))
MONITORS_EXECUTOR_MAX_WORKERS = int(os.environ.get("DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS", "10"))
