from typing import Dict

import datahub.telemetry.telemetry  # noqa: F401
import fastapi

import datahub_executor.coordinator.helpers
from datahub_executor.common.client.config.resolver import ExecutorConfigResolver


def _get_resolver() -> ExecutorConfigResolver:
    """
    Get the resolver instance to use for health checks.

    Tries to use the resolver from the worker module if available (when running
    in worker mode), otherwise creates its own instance. This ensures the health
    endpoint sees the retry state from the resolver that's actually being used
    for config fetching operations.
    """
    try:
        # Try to import the resolver from the worker module
        # This will succeed when running in worker mode or when the worker module is loaded
        from datahub_executor.worker.celery_sqs.app import _resolver as worker_resolver

        return worker_resolver
    except ImportError:
        # Fall back to creating our own instance if worker module is not available
        # This happens when running in coordinator-only mode
        return ExecutorConfigResolver()


# Module-level resolver instance for health checks
# Uses the worker's resolver if available, otherwise creates its own
_resolver = _get_resolver()

health_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@health_router.get("/health")
def health() -> Dict:
    """
    Determines whether the service is healthy.

    The service is considered healthy if:
    1. The manager and all fetchers are alive, OR
    2. The ExecutorConfigResolver is actively retrying (indicating transient GMS connectivity issues)

    This prevents premature pod restarts when the service is actively retrying to connect to GMS.
    """
    manager = datahub_executor.coordinator.helpers.manager
    manager_ok = manager.alive() if manager is not None else False

    # Check if ExecutorConfigResolver is actively retrying
    # If it is, consider the service as alive even if fetchers are stale
    # This prevents pod restarts during transient GMS connectivity issues
    is_actively_retrying = _resolver.is_actively_retrying(
        max_staleness_seconds=300
    )  # 5 minutes

    # Service is healthy if manager is ok OR if we're actively retrying
    ok = manager_ok or is_actively_retrying

    if not ok:
        raise fastapi.HTTPException(status_code=500, detail="Manager is not healthy")

    return {
        "healthy": ok,
        "manager_alive": manager_ok,
        "actively_retrying": is_actively_retrying,
    }
