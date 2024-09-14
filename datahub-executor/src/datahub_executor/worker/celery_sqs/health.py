import logging
from pathlib import Path

from datahub_executor.config import (
    DATAHUB_EXECUTOR_LIVENESS_HEARTBEAT_FILE,
    DATAHUB_EXECUTOR_READINESS_HEARTBEAT_FILE,
)

logger = logging.getLogger(__name__)


def kube_health_check() -> None:
    try:
        Path(DATAHUB_EXECUTOR_LIVENESS_HEARTBEAT_FILE).touch()
        Path(DATAHUB_EXECUTOR_READINESS_HEARTBEAT_FILE).touch()
    except Exception as e:
        logger.error(f"Failed to update health check file: {e}")
