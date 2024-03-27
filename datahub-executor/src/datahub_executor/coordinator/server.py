import logging
import time
import os
import signal

from fastapi import FastAPI
from threading import Thread

from datahub_executor.coordinator.assertion_endpoints import assertions_router
from datahub_executor.coordinator.health_endpoints import health_router
from datahub_executor.coordinator.helpers import (
    start_ingestion_pipeline,
    start_scheduler,
)
from datahub_executor.coordinator.logging import configure_logging
from datahub_executor.config import (
    DATAHUB_EXECUTOR_GRACEFUL_SHUTDOWN_PERIOD,
)

# Configure global logging.
configure_logging()

# Create FastAPI Server
app = FastAPI()

app.include_router(health_router)
app.include_router(assertions_router, prefix="/assertions")

logger = logging.getLogger(__name__)
sighandler = []  # type: ignore


@app.on_event("shutdown")
def shutdown_handler(*args, **kwargs):  # type: ignore
    global sighandler

    logger.info("Shutdown handler: uvicorn initiated shutdown")

    # Give all jobs some time to finish, then forcefully terminate the process
    def force_shutdown():
        time.sleep(DATAHUB_EXECUTOR_GRACEFUL_SHUTDOWN_PERIOD)
        logger.info("Shutdown handler: graceful period expired, force-exiting...")
        os.kill(os.getpid(), signal.SIGKILL)

    t = Thread(target=force_shutdown)
    t.start()

    for sighdlr in sighandler:
        try:
            sighdlr()
        except Exception:
            logger.error(
                "Shutdown handler: failed to execute one or more shutdown procedures"
            )


# Start the scheduler
start_scheduler(sighandler)

# Start the ingestion pipeline consumer
start_ingestion_pipeline(sighandler)
