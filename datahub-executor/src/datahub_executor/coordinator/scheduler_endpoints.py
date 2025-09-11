import logging
from typing import Any, Dict

import fastapi
from acryl.executor.request.execution_request import ExecutionRequest

from .scheduler_handlers import (
    handle_dump_jobs,
    handle_execute_async,
)

logger = logging.getLogger(__name__)

scheduler_router = fastapi.APIRouter(dependencies=[])


@scheduler_router.post("/execute_async")
def execute_async(
    execute_async_input: ExecutionRequest,
) -> bool:
    return handle_execute_async(execute_async_input)


@scheduler_router.get("/dump_jobs")
def dump_jobs() -> Dict[str, Any]:
    return handle_dump_jobs()


if __name__ == "__main__":
    # For development only
    pass
