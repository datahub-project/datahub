import logging

import prometheus_client

from datahub_executor.config import DATAHUB_EXECUTOR_PROMETHEUS_PORT

logger = logging.getLogger(__name__)

MONITORING_INITIALIZED = False


def monitoring_start() -> None:
    global MONITORING_INITIALIZED

    if not MONITORING_INITIALIZED:
        MONITORING_INITIALIZED = True
        try:
            prometheus_client.start_http_server(DATAHUB_EXECUTOR_PROMETHEUS_PORT)
        except Exception as e:
            logger.error(f"Unable to start prometheus server: {e}")
