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
    consumer_group: str = "generic-mae-consumer-job-client",
) -> None:
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return
    KAFKA_BROKER_CONTAINER: str = str(
        env_vars.get_kafka_broker_container() or infer_kafka_broker_container()
    )
    start_time = time.time()
    # get offsets
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
        # we want to sleep for an additional period of time for Elastic writes buffer to clear
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
