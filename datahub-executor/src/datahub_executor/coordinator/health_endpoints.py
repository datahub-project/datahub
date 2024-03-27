from typing import Dict

import datahub.telemetry.telemetry  # noqa: F401
import fastapi

import datahub_executor.coordinator.helpers

health_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@health_router.get("/health")
def health() -> Dict:
    """Determines whether the service is healthy. In the future, we'll introduce additional logic here."""

    manager = datahub_executor.coordinator.helpers.manager
    ok = manager.alive() if manager is not None else False
    if not ok:
        raise fastapi.HTTPException(status_code=500, detail="Manager is not healthy")

    return {"healthy": ok}
