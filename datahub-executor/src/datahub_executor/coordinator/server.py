import logging
import os
import signal
from threading import Event, Thread

from fastapi import FastAPI

from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.monitoring.base import monitoring_start
from datahub_executor.config import (
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_GRACEFUL_SHUTDOWN_PERIOD,
)
from datahub_executor.coordinator.assertion_endpoints import assertions_router
from datahub_executor.coordinator.health_endpoints import health_router
from datahub_executor.coordinator.helpers import (
    start_ingestion_pipeline,
    start_scheduler,
)
from datahub_executor.coordinator.logging import configure_logging

# Configure global logging.
configure_logging()

# Start prometheus server
monitoring_start()

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

    stop_event = Event()
    stop_event.clear()
    stop_succeeded = False

    # Give all jobs some time to finish, then forcefully terminate the process
    def force_shutdown() -> None:
        stop_event.clear()
        stop_event.wait(timeout=DATAHUB_EXECUTOR_GRACEFUL_SHUTDOWN_PERIOD)

        if not stop_succeeded:
            logger.info("Shutdown handler: graceful period expired, force-exiting...")
            os.kill(os.getpid(), signal.SIGKILL)

    t = Thread(target=force_shutdown)
    t.start()

    for sighdlr in sighandler:
        try:
            logger.error(f"shutting down: {sighdlr}")
            sighdlr()
        except Exception:
            logger.error(
                "Shutdown handler: failed to execute one or more shutdown procedures"
            )

    stop_succeeded = True
    stop_event.set()
    t.join()


# Create graph instance
graph = create_datahub_graph()

# Start discovery process
if DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED:
    discovery = DatahubExecutorDiscovery(graph)
    if discovery.is_backend_discovery_capable():
        sighandler.append(discovery.stop)
        discovery.start()
    else:
        logger.error("Discovery disabled: backend is not discovery-capable.")

# Start the scheduler
start_scheduler(graph, sighandler)

# Start the ingestion pipeline consumer
start_ingestion_pipeline(graph, discovery, sighandler)
