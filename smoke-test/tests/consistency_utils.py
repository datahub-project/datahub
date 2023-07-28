import time
import os
import subprocess

_ELASTIC_BUFFER_WRITES_TIME_IN_SEC: int = 1
USE_STATIC_SLEEP: bool = bool(os.getenv("USE_STATIC_SLEEP", False))
ELASTICSEARCH_REFRESH_INTERVAL_SECONDS: int = int(os.getenv("ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", 5))

def wait_for_writes_to_sync(max_timeout_in_sec: int = 120) -> None:
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return
    start_time = time.time()
    # get offsets
    lag_zero = False
    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)  # micro-sleep
        completed_process = subprocess.run(
            "docker exec broker /bin/kafka-consumer-groups --bootstrap-server broker:29092 --group generic-mae-consumer-job-client --describe | grep -v LAG | awk '{print $6}'",
            capture_output=True,
            shell=True,
            text=True,
        )

        result = str(completed_process.stdout)
        lines = result.splitlines()
        lag_values = [int(l) for l in lines if l != ""]
        maximum_lag = max(lag_values)
        if maximum_lag == 0:
            lag_zero = True

    if not lag_zero:
        logger.warning(f"Exiting early from waiting for elastic to catch up due to a timeout. Current lag is {lag_values}")
    else:
        # we want to sleep for an additional period of time for Elastic writes buffer to clear
        time.sleep(_ELASTIC_BUFFER_WRITES_TIME_IN_SEC)