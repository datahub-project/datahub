import contextlib
import dataclasses
import functools
import json
import logging
import os
import pathlib
import random
import signal
import subprocess
import time
from typing import Any, Iterator, Sequence

import pytest
import requests
import tenacity
from airflow.models.connection import Connection
from datahub.ingestion.sink.file import write_metadata_file
from datahub.testing.compare_metadata_json import assert_metadata_files_equal

from datahub_airflow_plugin._airflow_shims import (
    HAS_AIRFLOW_DAG_LISTENER_API,
    HAS_AIRFLOW_LISTENER_API,
    HAS_AIRFLOW_STANDALONE_CMD,
)

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)
IS_LOCAL = os.environ.get("CI", "false") == "false"

DAGS_FOLDER = pathlib.Path(__file__).parent / "dags"
GOLDENS_FOLDER = pathlib.Path(__file__).parent / "goldens"


@dataclasses.dataclass
class AirflowInstance:
    airflow_home: pathlib.Path
    airflow_port: int
    pid: int
    env_vars: dict

    username: str
    password: str

    metadata_file: pathlib.Path

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
    stop=tenacity.stop_after_delay(90),
    retry=tenacity.retry_if_exception_type(NotReadyError),
)
def _wait_for_dag_finish(
    airflow_instance: AirflowInstance, dag_id: str, require_success: bool
) -> None:
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
        if require_success:
            raise ValueError("DAG failed")
        # else - success is not required, so we're done.

    elif dag_run["state"] != "success":
        raise NotReadyError(f"DAG has not finished yet: {dag_run['state']}")


@contextlib.contextmanager
def _run_airflow(
    tmp_path: pathlib.Path, dags_folder: pathlib.Path, is_v1: bool
) -> Iterator[AirflowInstance]:
    airflow_home = tmp_path / "airflow_home"
    print(f"Using airflow home: {airflow_home}")

    if IS_LOCAL:
        airflow_port = 11792
    else:
        airflow_port = random.randint(10000, 12000)
    print(f"Using airflow port: {airflow_port}")

    datahub_connection_name = "datahub_file_default"
    meta_file = tmp_path / "datahub_metadata.json"

    environment = {
        **os.environ,
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": str(airflow_port),
        "AIRFLOW__WEBSERVER__BASE_URL": "http://airflow.example.com",
        # Point airflow to the DAGs folder.
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_folder),
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
        # Have the Airflow API use username/password authentication.
        "AIRFLOW__API__AUTH_BACKEND": "airflow.api.auth.backend.basic_auth",
        # Configure the datahub plugin and have it write the MCPs to a file.
        "AIRFLOW__CORE__LAZY_LOAD_PLUGINS": "False" if is_v1 else "True",
        "AIRFLOW__DATAHUB__CONN_ID": datahub_connection_name,
        f"AIRFLOW_CONN_{datahub_connection_name.upper()}": Connection(
            conn_id="datahub_file_default",
            conn_type="datahub-file",
            host=str(meta_file),
        ).get_uri(),
        # Configure fake credentials for the Snowflake connection.
        "AIRFLOW_CONN_MY_SNOWFLAKE": Connection(
            conn_id="my_snowflake",
            conn_type="snowflake",
            login="fake_username",
            password="fake_password",
            schema="DATAHUB_TEST_SCHEMA",
            extra={
                "account": "fake_account",
                "database": "DATAHUB_TEST_DATABASE",
                "warehouse": "fake_warehouse",
                "role": "fake_role",
                "insecure_mode": "true",
            },
        ).get_uri(),
        "AIRFLOW_CONN_MY_SQLITE": Connection(
            conn_id="my_sqlite",
            conn_type="sqlite",
            host=str(tmp_path / "my_sqlite.db"),
        ).get_uri(),
        # Convenience settings.
        "AIRFLOW__DATAHUB__LOG_LEVEL": "DEBUG",
        "AIRFLOW__DATAHUB__DEBUG_EMITTER": "True",
        "SQLALCHEMY_SILENCE_UBER_WARNING": "1",
    }

    if not HAS_AIRFLOW_STANDALONE_CMD:
        raise pytest.skip("Airflow standalone command is not available")

    # Start airflow in a background subprocess.
    airflow_process = subprocess.Popen(
        ["airflow", "standalone"],
        env=environment,
    )

    try:
        _wait_for_airflow_healthy(airflow_port)
        print("Airflow is ready!")

        # Sleep for a few seconds to make sure the other Airflow processes are ready.
        time.sleep(3)

        # Create an extra "airflow" user for easy testing.
        if IS_LOCAL:
            print("Creating an extra test user...")
            subprocess.check_call(
                [
                    # fmt: off
                    "airflow", "users", "create",
                    "--username", "airflow",
                    "--password", "airflow",
                    "--firstname", "admin",
                    "--lastname", "admin",
                    "--role", "Admin",
                    "--email", "airflow@example.com",
                    # fmt: on
                ],
                env=environment,
            )

        # Sanity check that the plugin got loaded.
        if not is_v1:
            print("[debug] Listing loaded plugins")
            subprocess.check_call(
                ["airflow", "plugins", "-v"],
                env=environment,
            )

        # Load the admin user's password. This is generated by the
        # `airflow standalone` command, and is different from the
        # airflow user that we create when running locally.
        airflow_username = "admin"
        airflow_password = (airflow_home / "standalone_admin_password.txt").read_text()

        airflow_instance = AirflowInstance(
            airflow_home=airflow_home,
            airflow_port=airflow_port,
            pid=airflow_process.pid,
            env_vars=environment,
            username=airflow_username,
            password=airflow_password,
            metadata_file=meta_file,
        )

        yield airflow_instance
    finally:
        try:
            # Attempt a graceful shutdown.
            print("Shutting down airflow...")
            airflow_process.send_signal(signal.SIGINT)
            airflow_process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            # If the graceful shutdown failed, kill the process.
            print("Hard shutting down airflow...")
            airflow_process.kill()
            airflow_process.wait(timeout=3)


def check_golden_file(
    pytestconfig: pytest.Config,
    output_path: pathlib.Path,
    golden_path: pathlib.Path,
    ignore_paths: Sequence[str] = (),
) -> None:
    update_golden = pytestconfig.getoption("--update-golden-files")

    assert_metadata_files_equal(
        output_path=output_path,
        golden_path=golden_path,
        update_golden=update_golden,
        copy_output=False,
        ignore_paths=ignore_paths,
        ignore_order=False,
    )


@dataclasses.dataclass
class DagTestCase:
    dag_id: str
    success: bool = True

    v2_only: bool = False


test_cases = [
    DagTestCase("simple_dag"),
    DagTestCase("basic_iolets"),
    DagTestCase("snowflake_operator", success=False, v2_only=True),
    DagTestCase("sqlite_operator", v2_only=True),
]


@pytest.mark.parametrize(
    ["golden_filename", "test_case", "is_v1"],
    [
        # On Airflow <= 2.2, test plugin v1.
        *[
            pytest.param(
                f"v1_{test_case.dag_id}",
                test_case,
                True,
                id=f"v1_{test_case.dag_id}",
                marks=pytest.mark.skipif(
                    HAS_AIRFLOW_LISTENER_API,
                    reason="Not testing plugin v1 on newer Airflow versions",
                ),
            )
            for test_case in test_cases
            if not test_case.v2_only
        ],
        *[
            pytest.param(
                # On Airflow 2.3-2.4, test plugin v2 without dataFlows.
                f"v2_{test_case.dag_id}"
                if HAS_AIRFLOW_DAG_LISTENER_API
                else f"v2_{test_case.dag_id}_no_dag_listener",
                test_case,
                False,
                id=f"v2_{test_case.dag_id}"
                if HAS_AIRFLOW_DAG_LISTENER_API
                else f"v2_{test_case.dag_id}_no_dag_listener",
                marks=pytest.mark.skipif(
                    not HAS_AIRFLOW_LISTENER_API,
                    reason="Cannot test plugin v2 without the Airflow plugin listener API",
                ),
            )
            for test_case in test_cases
        ],
    ],
)
def test_airflow_plugin(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
    golden_filename: str,
    test_case: DagTestCase,
    is_v1: bool,
) -> None:
    # This test:
    # - Configures the plugin.
    # - Starts a local airflow instance in a subprocess.
    # - Runs a DAG that uses an operator supported by the extractor.
    # - Waits for the DAG to complete.
    # - Validates the metadata generated against a golden file.

    if not is_v1 and not test_case.success and not HAS_AIRFLOW_DAG_LISTENER_API:
        # Saw a number of issues in CI where this would fail to emit the last events
        # due to an error in the SQLAlchemy listener. This never happened locally for me.
        pytest.skip("Cannot test failure cases without the Airflow DAG listener API")

    golden_path = GOLDENS_FOLDER / f"{golden_filename}.json"
    dag_id = test_case.dag_id

    with _run_airflow(
        tmp_path, dags_folder=DAGS_FOLDER, is_v1=is_v1
    ) as airflow_instance:
        print(f"Running DAG {dag_id}...")
        subprocess.check_call(
            [
                "airflow",
                "dags",
                "trigger",
                "--exec-date",
                "2023-09-27T21:34:38+00:00",
                "-r",
                "manual_run_test",
                dag_id,
            ],
            env=airflow_instance.env_vars,
        )

        print("Waiting for DAG to finish...")
        _wait_for_dag_finish(
            airflow_instance, dag_id, require_success=test_case.success
        )

        print("Sleeping for a few seconds to let the plugin finish...")
        time.sleep(10)

    _sanitize_output_file(airflow_instance.metadata_file)

    check_golden_file(
        pytestconfig=pytestconfig,
        output_path=airflow_instance.metadata_file,
        golden_path=golden_path,
        ignore_paths=[
            # Timing-related items.
            # r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['start_date'\]",
            # r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['end_date'\]",
            # r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['duration'\]",
            # TODO: If we switched to Git urls, maybe we could get this to work consistently.
            # r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['fileloc'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['openlineage_.*'\]",
        ],
    )


def _sanitize_output_file(output_path: pathlib.Path) -> None:
    # Overwrite some custom properties in the output file to make it easier to compare.

    props_job = {
        "fileloc": "<fileloc>",
    }
    props_process = {
        "start_date": "<start_date>",
        "end_date": "<end_date>",
        "duration": "<duration>",
    }

    def _sanitize(obj: Any) -> None:
        if isinstance(obj, dict) and "customProperties" in obj:
            replacement_props = (
                props_process if "run_id" in obj["customProperties"] else props_job
            )
            obj["customProperties"] = {
                k: replacement_props.get(k, v)
                for k, v in obj["customProperties"].items()
            }
        elif isinstance(obj, dict):
            for v in obj.values():
                _sanitize(v)
        elif isinstance(obj, list):
            for v in obj:
                _sanitize(v)

    objs = json.loads(output_path.read_text())
    _sanitize(objs)

    write_metadata_file(output_path, objs)


if __name__ == "__main__":
    # When run directly, just set up a local airflow instance.
    import tempfile

    with _run_airflow(
        tmp_path=pathlib.Path(tempfile.mkdtemp("airflow-plugin-test")),
        dags_folder=DAGS_FOLDER,
        is_v1=not HAS_AIRFLOW_LISTENER_API,
    ) as airflow_instance:
        # input("Press enter to exit...")
        breakpoint()
        print("quitting airflow")
