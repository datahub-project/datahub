import sys
import logging
import signal

from uvicorn.main import Server

from datahub_monitors.service.action_pipeline.pipeline import start_async_action_pipeline
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

logger = logging.getLogger(__name__)
sighandler = []

@app.on_event("shutdown")
def shutdown_handler(*args, **kwargs):
    global sighandler

    logger.info('Shutdown handler: uvicorn initiated shutdown')

    for sighdlr in sighandler:
        try:
            sighdlr()
        except Exception:
            logger.error("Shutdown handler: failed to execute one or more shutdown procedures")

# Start the async monitors
start_async_execution_request_manager(sighandler)

# Start the action pipeline
start_async_action_pipeline(sighandler)
