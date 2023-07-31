from typing import Dict

import datahub.telemetry.telemetry  # noqa: F401
import fastapi

from datahub_monitors.app.monitors import manager

health_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@health_router.get("/health")
def health() -> Dict:
    """Determines whether the service is healthy. In the future, we'll introduce additional logic here."""
    if manager is not None:
        return {"healthy": True}
    return {"healthy": False}
