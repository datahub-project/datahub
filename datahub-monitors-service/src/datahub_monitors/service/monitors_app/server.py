from datahub_monitors.service.action_pipeline.pipeline import start_action_pipeline
from datahub_monitors.service.monitors_app.app import app
from datahub_monitors.service.monitors_app.health import health_router
from datahub_monitors.service.monitors_app.logging import configure_logging
from datahub_monitors.service.monitors_app.monitors import (
    start_async_execution_request_manager,
)
from datahub_monitors.service.monitors_app.routes import assertions_router

# Configure global logging.
configure_logging()

app.include_router(health_router)
app.include_router(assertions_router, prefix="/assertions")

# Start the async monitors
start_async_execution_request_manager()

# Start the action pipeline
start_action_pipeline()
