import logging
import subprocess
import time

from tests.utilities import env_vars

USE_STATIC_SLEEP: bool = env_vars.get_use_static_sleep()
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


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    mcp_only: bool = False,
    mae_only: bool = False,
    cdc_only: bool = False,
    consumer_group: str | None = None,  # Deprecated: for backward compatibility
) -> None:
    """Wait for Kafka consumer lag to reach zero.

    By default, waits for ALL consumers (MAE, MCP, CDC) to catch up before returning.
    This ensures writes are fully propagated through the system.

    Args:
        max_timeout_in_sec: Maximum time to wait in seconds (default: 120)
        mcp_only: If True, only wait for MCP consumer (for ingestion pipeline tests)
        mae_only: If True, only wait for MAE consumer (for entity update tests)
        cdc_only: If True, only wait for CDC consumer (for change data capture tests)
        consumer_group: (Deprecated) Specific consumer group to wait for - use flags instead

    Note:
        - Default (all False): Waits for MAE + MCP + CDC consumers (safest, most thorough)
        - mcp_only=True: Fast path for datahub-rest sink / ingestion pipeline writes
        - mae_only=True: For legacy MAE-only writes
        - cdc_only=True: For CDC-specific writes
        - Only one flag should be True at a time (if multiple are True, first one wins)
        - consumer_group: Backward compatibility - waits for specific consumer
        - If USE_STATIC_SLEEP is True, uses fixed sleep instead of checking lag
    """
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    # Build consumer group pattern based on flags (first flag wins if multiple are set)
    if consumer_group is not None:
        # Backward compatibility: use specific consumer group
        consumer_pattern = consumer_group
    elif mcp_only:
        consumer_pattern = "generic-mcp-consumer-job-client"
    elif mae_only:
        consumer_pattern = "generic-mae-consumer-job-client"
    elif cdc_only:
        consumer_pattern = "cdc-consumer-job-client"
    else:
        # Default: wait for ALL consumers (most thorough)
        consumer_pattern = "(generic-mae-consumer-job-client|cdc-consumer-job-client|generic-mcp-consumer-job-client)"

    KAFKA_BROKER_CONTAINER: str = str(
        env_vars.get_kafka_broker_container() or infer_kafka_broker_container()
    )
    start_time = time.time()
    lag_zero = False

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)  # micro-sleep

        cmd = (
            f"docker exec {KAFKA_BROKER_CONTAINER} /bin/kafka-consumer-groups "
            f"--bootstrap-server {KAFKA_BOOTSTRAP_SERVER} --all-groups --describe | "
            f"grep -E '{consumer_pattern}' | awk '{{print $6}}'"
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
            if not lag_values:
                # No lag data found - consumers might not have started yet
                continue
            maximum_lag = max(lag_values)
            if maximum_lag == 0:
                lag_zero = True
        except ValueError:
            logger.warning(
                f"Error reading kafka lag using command: {cmd}", exc_info=True
            )

    if not lag_zero:
        logger.warning(
            f"Exiting early from waiting for consumers to catch up due to timeout. "
            f"Current lag: {lag_values if 'lag_values' in locals() else 'unknown'}"
        )
    else:
        # Sleep for additional time for Elasticsearch writes buffer to clear
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
