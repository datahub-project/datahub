import os


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


def env_to_int(varname: str, default_val: int) -> int:
    val = os.environ.get(varname)
    if val is None:
        return default_val
    if val.isdigit():
        return int(val)
    return default_val


def datahub_url() -> str:
    url = os.environ.get("DATAHUB_GMS_URL")
    if url is not None:
        return url
    return (
        f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://"
        f"{os.environ.get('DATAHUB_GMS_HOST', 'localhost')}:{os.environ.get('DATAHUB_GMS_PORT', 8080)}"
    )


EXECUTOR_MODE = os.getenv("DATAHUB_EXECUTOR_MODE", "worker")

MONITORS_ENABLED = string_to_bool(
    os.getenv("DATAHUB_EXECUTOR_MONITORS_ENABLED", "True")
)
INGESTION_ENABLED = string_to_bool(
    os.getenv("DATAHUB_EXECUTOR_INGESTION_ENABLED", "True")
)
EMBEDDED_WORKER_ENABLED = string_to_bool(
    os.getenv("DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED", "True")
)
EMBEDDED_WORKER_ID = os.getenv("DATAHUB_EXECUTOR_EMBEDDED_WORKER_ID", "default")

EXECUTOR_ID = os.environ.get("DATAHUB_EXECUTOR_WORKER_ID", "default")

DATAHUB_ACCESS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", None)
DATAHUB_SERVER = datahub_url()

ACTIONS_PIPELINE_CONFIG_PATH = os.environ.get(
    "DATAHUB_EXECUTOR_ACTIONS_CONFIG_PATH", "/etc/datahub/actions-pipeline/config.yaml"
)
ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS = env_to_int(
    "DATAHUB_EXECUTOR_ACTIONS_MAX_WORKERS", 4
)
ACTIONS_PIPELINE_SIGNAL_POLL_INTERVAL = env_to_int(
    "DATAHUB_EXECUTOR_ACTIONS_SIGNAL_POLL_INTERVAL", 2
)
MONITORS_EXECUTOR_MAX_WORKERS = env_to_int("DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS", 10)


READINESS_HEARTBEAT_FILE = "/tmp/worker_readiness_heartbeat"
LIVENESS_HEARTBEAT_FILE = "/tmp/worker_liveness_heartbeat"
