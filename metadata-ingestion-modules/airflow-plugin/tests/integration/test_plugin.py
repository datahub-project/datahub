import contextlib
import dataclasses
import functools
import logging
import os
import pathlib
import signal
import subprocess
from typing import Iterator

import requests
import tenacity
from airflow.models.connection import Connection

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class AirflowInstance:
    airflow_home: pathlib.Path
    airflow_port: int
    pid: int
    env_vars: dict

    username: str
    password: str

    @property
    def airflow_url(self) -> str:
        return f"http://localhost:{self.airflow_port}"

    @functools.cached_property
    def session(self) -> requests.Session:
        session = requests.Session()
        session.auth = (self.username, self.password)
        return session


@tenacity.retry(
    reraise=True,
    wait=tenacity.wait_fixed(1),
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(
        (AssertionError, requests.exceptions.RequestException)
    ),
)
def _wait_for_airflow_healthy(airflow_port: int) -> None:
    print("Checking if Airflow is ready...")
    res = requests.get(f"http://localhost:{airflow_port}/health", timeout=5)
    res.raise_for_status()

    airflow_health = res.json()
    assert airflow_health["metadatabase"]["status"] == "healthy"
    assert airflow_health["scheduler"]["status"] == "healthy"


class NotReadyError(Exception):
    pass


@tenacity.retry(
    reraise=True,
    wait=tenacity.wait_fixed(1),
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(NotReadyError),
)
def _wait_for_dag_finish(airflow_instance: AirflowInstance, dag_id: str) -> None:
    print("Checking if DAG is finished")
    res = airflow_instance.session.get(
        f"{airflow_instance.airflow_url}/api/v1/dags/{dag_id}/dagRuns", timeout=5
    )
    res.raise_for_status()

    dag_runs = res.json()["dag_runs"]
    if not dag_runs:
        raise NotReadyError("No DAG runs found")

    dag_run = dag_runs[0]
    if dag_run["state"] == "failed":
        raise ValueError("DAG failed")

    if dag_run["state"] != "success":
        raise NotReadyError(f"DAG has not finished yet: {dag_run['state']}")


@contextlib.contextmanager
def _run_airflow(
    tmp_path: pathlib.Path, dags_folder: pathlib.Path
) -> Iterator[AirflowInstance]:
    airflow_home = tmp_path / "airflow_home"
    print(f"Using airflow home: {airflow_home}")

    # airflow_port = random.randint(10000, 12000)
    airflow_port = 11792
    print(f"Using airflow port: {airflow_port}")

    datahub_connection_name = "datahub_file_default"
    meta_file = tmp_path / "datahub_metadata.json"

    environment = {
        **os.environ,
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": str(airflow_port),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_folder),
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
        # Have the datahub plugin write metadata to a file.
        "AIRFLOW__DATAHUB__CONN_ID": datahub_connection_name,
        f"AIRFLOW_CONN_{datahub_connection_name.upper()}": Connection(
            conn_id="datahub_file_default",
            conn_type="datahub-file",
            host=str(meta_file),
        ).get_uri(),
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

        yield AirflowInstance(
            airflow_home=airflow_home,
            airflow_port=airflow_port,
            pid=airflow_process.pid,
            env_vars=environment,
            username=airflow_username,
            password=airflow_password,
        )
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

    with _run_airflow(tmp_path, dags_folder=dags_folder) as airflow_instance:
        dag_id = "simple_dag"

        breakpoint()

        print(f"Running DAG {dag_id}...")
        subprocess.check_call(
            [
                "airflow",
                "dags",
                "trigger",
                "-r",
                "manual_run_test",
                dag_id,
            ],
            env=airflow_instance.env_vars,
        )

        breakpoint()

        print("Waiting for DAG to finish...")
        _wait_for_dag_finish(airflow_instance, dag_id)

        breakpoint()

        print("done")
