import logging
from typing import Any, Dict, Optional

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

logger = logging.getLogger(__name__)

scheduler: Optional[ExecutionRequestScheduler] = None


def handle_execute_async(execute_async_input: ExecutionRequest) -> bool:
    if scheduler is None:
        return False
    try:
        scheduler.submit_execution_request(execute_async_input)
    except Exception:
        return False
    return True


def handle_dump_jobs() -> Dict[str, Any]:
    if scheduler is None:
        return {}
    try:
        return scheduler.dump_jobs()
    except Exception:
        logger.exception("Failed to dump scheduler jobs")
        return {}
