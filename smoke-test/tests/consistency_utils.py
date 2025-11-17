import logging
import time
from typing import Optional

import requests

from tests.utilities import env_vars

USE_STATIC_SLEEP: bool = env_vars.get_use_static_sleep()
ELASTICSEARCH_REFRESH_INTERVAL_SECONDS: int = (
    env_vars.get_elasticsearch_refresh_interval_seconds()
)

logger = logging.getLogger(__name__)


def get_kafka_consumer_lag_from_api() -> Optional[int]:
    """
    Fetch Kafka consumer lag using DataHub's OpenAPI endpoint.
    Returns the maximum lag across all consumer groups (MCP, MCL, MCL-timeseries).
    Returns None if the API call fails.
    """
    gms_url = env_vars.get_gms_url()
    if not gms_url:
        logger.warning("GMS URL not configured, cannot fetch Kafka lag from API")
        return None

    # Get admin credentials
    admin_username = env_vars.get_admin_username()
    admin_password = env_vars.get_admin_password()

    # Define the endpoints to check
    endpoints = [
        "/openapi/operations/kafka/mcp/consumer/offsets",
        "/openapi/operations/kafka/mcl/consumer/offsets",
        "/openapi/operations/kafka/mcl-timeseries/consumer/offsets",
    ]

    max_lag = 0
    for endpoint in endpoints:
        url = f"{gms_url}{endpoint}?skipCache=true&detailed=false"
        try:
            response = requests.get(
                url,
                auth=(admin_username, admin_password),
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                # Parse the response structure:
                # {
                #   "consumer-group-id": {
                #     "topic-name": {
                #       "metrics": {
                #         "maxLag": 500
                #       }
                #     }
                #   }
                # }
                for consumer_group in data.values():
                    if isinstance(consumer_group, dict):
                        for topic_info in consumer_group.values():
                            if isinstance(topic_info, dict) and "metrics" in topic_info:
                                metrics = topic_info["metrics"]
                                if metrics and "maxLag" in metrics:
                                    lag = metrics["maxLag"]
                                    max_lag = max(max_lag, lag)
            else:
                logger.warning(
                    f"Failed to fetch Kafka lag from {endpoint}: HTTP {response.status_code}"
                )
        except Exception as e:
            logger.warning(f"Error fetching Kafka lag from {endpoint}: {e}")

    return max_lag


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    consumer_group: str = "generic-mae-consumer-job-client",
) -> None:
    """
    Wait for Kafka consumer lag to reach zero, indicating all writes have been processed.
    Uses DataHub's OpenAPI endpoint to check consumer lag instead of Kafka CLI.
    """
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    start_time = time.time()
    lag_zero = False
    current_lag = None

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)  # micro-sleep

        current_lag = get_kafka_consumer_lag_from_api()
        if current_lag is not None:
            if current_lag == 0:
                lag_zero = True
        else:
            logger.warning("Unable to fetch Kafka lag from API")

    if not lag_zero:
        logger.warning(
            f"Exiting early from waiting for elastic to catch up due to a timeout. Current lag is {current_lag}"
        )
    else:
        # we want to sleep for an additional period of time for Elastic writes buffer to clear
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
