import contextlib
import logging
import os
import pathlib
import random
import signal
import subprocess
import time
from typing import Iterator

import requests
import tenacity

logger = logging.getLogger(__name__)


@tenacity.retry(
    reraise=True,
    wait=tenacity.wait_fixed(1),
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(
        AssertionError, requests.exceptions.RequestException
    ),
)
def _wait_for_airflow_healthy(airflow_port: int) -> None:
    print("Checking if Airflow is ready...")
    res = requests.get(f"http://localhost:{airflow_port}/health", timeout=5)
    res.raise_for_status()

    airflow_health = res.json()
    assert airflow_health["metadatabase"]["status"] == "healthy"
    assert airflow_health["scheduler"]["status"] == "healthy"


@contextlib.contextmanager
def _run_airflow(tmp_path: pathlib.Path, dags_folder: pathlib.Path) -> Iterator[None]:
    airflow_home = tmp_path / "airflow_home"
    print(f"Using airflow home: {airflow_home}")

    airflow_port = random.randint(10000, 12000)
    print(f"Using airflow port: {airflow_port}")

    environment = {
        **os.environ,
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": str(airflow_port),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_folder),
        # TODO change the datahub plugin connection id
        # TODO set up a datahub file hook
    }

    # Start airflow in a background subprocess.
    airflow_process = subprocess.Popen(
        ["airflow", "standalone"],
        env=environment,
    )

    try:
        _wait_for_airflow_healthy(airflow_port)
        print("Airflow is ready!")

        airflow_username = "admin"
        airflow_password = (airflow_home / "standalone_admin_password.txt").read_text()

        yield
    finally:
        # Attempt a graceful shutdown.
        print("Shutting down airflow...")
        airflow_process.send_signal(signal.SIGINT)
        airflow_process.wait(timeout=30)

        # If the graceful shutdown failed, kill the process.
        airflow_process.kill()
        airflow_process.wait(timeout=3)


def test_airflow_plugin_v2(tmp_path: pathlib.Path) -> None:
    # This test:
    # - Configures the plugin.
    # - Starts a local airflow instance in a subprocess.
    # - Runs a DAG that uses an operator supported by the extractor.
    # - Waits for the DAG to complete.
    # - Checks that the metadata was emitted to DataHub.

    # TODO cleanup old running airflow instances

    dags_folder = pathlib.Path(__file__).parent / "dags"

    with _run_airflow(tmp_path, dags_folder=dags_folder):
        breakpoint()
        print("done")
