import logging
import subprocess
import time
from typing import Optional

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from tests.utilities import env_vars

USE_STATIC_SLEEP: bool = env_vars.get_use_static_sleep()
USE_KAFKA_API_FOR_LAG: bool = env_vars.get_use_kafka_api_for_lag()
ELASTICSEARCH_REFRESH_INTERVAL_SECONDS: int = (
    env_vars.get_elasticsearch_refresh_interval_seconds()
)
KAFKA_BOOTSTRAP_SERVER: str = env_vars.get_kafka_bootstrap_server()

logger = logging.getLogger(__name__)


def infer_kafka_broker_container() -> str:
    cmd = "docker ps --format '{{.Names}}' | grep broker"
    completed_process = subprocess.run(
        cmd,
        capture_output=True,
        shell=True,
        text=True,
    )
    result = str(completed_process.stdout)
    lines = result.splitlines()
    if len(lines) == 0:
        raise ValueError("No Kafka broker containers found")
    return lines[0]


def _get_max_lag_from_offsets(offsets_response: dict) -> Optional[int]:
    """
    Extract the maximum lag across all consumer groups and topics.

    Args:
        offsets_response: Response from graph.get_kafka_consumer_offsets()

    Returns:
        Maximum lag value, or None if no lag data found
    """
    max_lag = None

    for stream_key, stream_data in offsets_response.items():
        if "errors" in stream_data:
            logger.warning(f"Error in {stream_key} offsets: {stream_data['errors']}")
            continue

        # stream_data is {consumer_group_id: {topic: {metrics: {...}}}}
        for consumer_group_id, topics_data in stream_data.items():
            for topic_name, topic_info in topics_data.items():
                metrics = topic_info.get("metrics", {})
                topic_max_lag = metrics.get("maxLag")

                if topic_max_lag is not None:
                    max_lag = max(max_lag or 0, topic_max_lag)
                    logger.debug(
                        f"Found lag: stream={stream_key}, consumer_group={consumer_group_id}, "
                        f"topic={topic_name}, maxLag={topic_max_lag}"
                    )

    return max_lag


def _wait_using_kafka_api(max_timeout_in_sec: int) -> bool:
    """
    Wait for Kafka consumer lag to reach zero using DataHub's Kafka API.

    Args:
        max_timeout_in_sec: Maximum time to wait in seconds

    Returns:
        True if lag reached zero, False if timed out or API unavailable
    """
    gms_url = env_vars.get_gms_url()
    if not gms_url:
        logger.warning("DATAHUB_GMS_URL not set, cannot use Kafka API")
        return False

    try:
        graph = DataHubGraph(
            config=DatahubClientConfig(
                server=gms_url,
                disable_ssl_verification=True,
            )
        )
    except Exception as e:
        logger.warning(f"Failed to create DataHub graph client: {e}")
        return False

    start_time = time.time()

    while (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)  # micro-sleep

        try:
            offsets_response = graph.get_kafka_consumer_offsets()
            max_lag = _get_max_lag_from_offsets(offsets_response)

            if max_lag is None:
                # No consumer offsets found - consumer groups may not have started yet
                logger.info("No Kafka consumer lag data found via API")
                return False

            logger.debug(f"Current maximum lag across all consumers: {max_lag}")

            if max_lag == 0:
                logger.debug("All Kafka consumers have caught up (lag = 0)")
                return True

        except Exception as e:
            logger.warning(f"Error reading Kafka lag using API: {e}")
            continue

    logger.warning("Timeout waiting for Kafka lag using API")
    return False


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    consumer_group: str = "generic-mae-consumer-job-client",
) -> None:
    """
    Wait for Kafka consumer lag to reach zero across all DataHub consumers.

    By default, uses the DataHub Kafka API. If unavailable or disabled, falls back to docker exec.

    Args:
        max_timeout_in_sec: Maximum time to wait in seconds (default: 120)
        consumer_group: Consumer group to monitor for docker exec fallback
    """
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    # Try using Kafka API if enabled
    if USE_KAFKA_API_FOR_LAG:
        if _wait_using_kafka_api(max_timeout_in_sec):
            # Success! Sleep for Elasticsearch refresh interval
            time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
            return

        # API failed or unavailable - fall through to docker exec

    # Original docker exec implementation
    KAFKA_BROKER_CONTAINER: str = str(
        env_vars.get_kafka_broker_container() or infer_kafka_broker_container()
    )
    start_time = time.time()
    lag_zero = False

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)  # micro-sleep

        cmd = (
            f"docker exec {KAFKA_BROKER_CONTAINER} /bin/kafka-consumer-groups --bootstrap-server {KAFKA_BOOTSTRAP_SERVER} --all-groups --describe | grep -E '({consumer_group}|cdc-consumer-job-client)'  "
            + "| awk '{print $6}'"
        )
        try:
            completed_process = subprocess.run(
                cmd,
                capture_output=True,
                shell=True,
                text=True,
            )
            result = str(completed_process.stdout)
            lines = result.splitlines()
            lag_values = [int(line) for line in lines if line != ""]
            maximum_lag = max(lag_values)
            if maximum_lag == 0:
                lag_zero = True
        except ValueError:
            logger.warning(
                f"Error reading kafka lag using command: {cmd}", exc_info=True
            )

    if not lag_zero:
        logger.warning(
            f"Exiting early from waiting for elastic to catch up due to a timeout. Current lag is {lag_values}"
        )
    else:
        # Sleep for Elasticsearch refresh interval
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
