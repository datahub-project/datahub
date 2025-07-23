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
import textwrap
import time
from typing import Any, Iterator, Optional, Sequence

import pytest
import requests
import tenacity
from airflow.models.connection import Connection

from datahub.ingestion.sink.file import write_metadata_file
from datahub.testing.compare_metadata_json import assert_metadata_files_equal

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)
IS_LOCAL = os.environ.get("CI", "false") == "false"

DAGS_FOLDER = pathlib.Path(__file__).parent / "dags"
GOLDENS_FOLDER = pathlib.Path(__file__).parent / "goldens"

DAG_TO_SKIP_INGESTION = "dag_to_skip"

PLATFORM_INSTANCE = "myairflow"


@dataclasses.dataclass
class AirflowInstance:
    airflow_home: pathlib.Path
    airflow_port: int
    pid: int
    env_vars: dict

    username: str
    password: str

    metadata_file: pathlib.Path
    metadata_file2: pathlib.Path

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


@tenacity.retry(
    reraise=True,
    wait=tenacity.wait_fixed(1),
    stop=tenacity.stop_after_delay(90),
    retry=tenacity.retry_if_exception_type(NotReadyError),
)
def _wait_for_dag_to_load(airflow_instance: AirflowInstance, dag_id: str) -> None:
    print(f"Checking if DAG {dag_id} was loaded")
    res = airflow_instance.session.get(
        url=f"{airflow_instance.airflow_url}/api/v1/dags",
        timeout=5,
    )
    res.raise_for_status()

    if len(list(filter(lambda x: x["dag_id"] == dag_id, res.json()["dags"]))) == 0:
        raise NotReadyError("DAG was not loaded yet")


def _dump_dag_logs(airflow_instance: AirflowInstance, dag_id: str) -> None:
    # Get the dag run info
    res = airflow_instance.session.get(
        f"{airflow_instance.airflow_url}/api/v1/dags/{dag_id}/dagRuns", timeout=5
    )
    res.raise_for_status()
    dag_run = res.json()["dag_runs"][0]
    dag_run_id = dag_run["dag_run_id"]

    # List the tasks in the dag run
    res = airflow_instance.session.get(
        f"{airflow_instance.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        timeout=5,
    )
    res.raise_for_status()
    task_instances = res.json()["task_instances"]

    # Sort tasks by start_date to maintain execution order
    task_instances.sort(key=lambda x: x["start_date"] or "")

    print(f"\nTask execution order for DAG {dag_id}:")
    for task in task_instances:
        task_id = task["task_id"]
        state = task["state"]
        try_number = task.get("try_number", 1)

        task_header = f"Task: {task_id} (State: {state}; Try: {try_number})"

        # Get logs for the task's latest try number
        try:
            res = airflow_instance.session.get(
                f"{airflow_instance.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
                f"/taskInstances/{task_id}/logs/{try_number}",
                params={"full_content": "true"},
                timeout=5,
            )
            res.raise_for_status()
            print(f"\n=== {task_header} ===\n{textwrap.indent(res.text, '    ')}")
        except Exception as e:
            print(f"Failed to fetch logs for {task_header}: {e}")


@contextlib.contextmanager
def _run_airflow(
    tmp_path: pathlib.Path,
    dags_folder: pathlib.Path,
    multiple_connections: bool,
    platform_instance: Optional[str],
    enable_datajob_lineage: bool,
) -> Iterator[AirflowInstance]:
    airflow_home = tmp_path / "airflow_home"
    print(f"Using airflow home: {airflow_home}")

    if IS_LOCAL:
        airflow_port = 11792
    else:
        airflow_port = random.randint(10000, 12000)
    print(f"Using airflow port: {airflow_port}")

    datahub_connection_name = "datahub_file_default"
    datahub_connection_name_2 = "datahub_file_default_2"
    meta_file = tmp_path / "datahub_metadata.json"
    meta_file2 = tmp_path / "datahub_metadata_2.json"

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
        "AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.basic_auth",
        # Configure the datahub plugin and have it write the MCPs to a file.
        "AIRFLOW__DATAHUB__CONN_ID": f"{datahub_connection_name}, {datahub_connection_name_2}"
        if multiple_connections
        else datahub_connection_name,
        "AIRFLOW__DATAHUB__DAG_FILTER_STR": f'{{ "deny": ["{DAG_TO_SKIP_INGESTION}"] }}',
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
        "AIRFLOW_CONN_MY_AWS": Connection(
            conn_id="my_aws",
            conn_type="aws",
            extra={
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
        ).get_uri(),
        "AIRFLOW_CONN_MY_BIGQUERY": Connection(
            conn_id="my_bigquery",
            conn_type="google_cloud_platform",
            extra={
                "project": "test_project",
                "keyfile_dict": {
                    "type": "service_account",
                    "project_id": "test_project",
                    "private_key_id": "test_key_id",
                    "private_key": "-----BEGIN PRIVATE KEY-----\nfake_key\n-----END PRIVATE KEY-----\n",
                    "client_email": "test@test_project.iam.gserviceaccount.com",
                    "client_id": "123456789",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                },
            },
        ).get_uri(),
        "AIRFLOW_CONN_MY_SQLITE": Connection(
            conn_id="my_sqlite",
            conn_type="sqlite",
            host=str(tmp_path / "my_sqlite.db"),
        ).get_uri(),
        # Ensure that the plugin waits for metadata to be written.
        # Note that we could also disable the RUN_IN_THREAD entirely,
        # but I want to minimize the difference between CI and prod.
        "DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT": "30",
        # Convenience settings.
        "AIRFLOW__DATAHUB__LOG_LEVEL": "DEBUG",
        "AIRFLOW__DATAHUB__DEBUG_EMITTER": "True",
        "SQLALCHEMY_SILENCE_UBER_WARNING": "1",
        "AIRFLOW__DATAHUB__ENABLE_DATAJOB_LINEAGE": "true"
        if enable_datajob_lineage
        else "false",
    }

    if platform_instance:
        environment["AIRFLOW__DATAHUB__PLATFORM_INSTANCE"] = platform_instance

    if multiple_connections:
        environment[f"AIRFLOW_CONN_{datahub_connection_name_2.upper()}"] = Connection(
            conn_id="datahub_file_default2",
            conn_type="datahub-file",
            host=str(meta_file2),
        ).get_uri()

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
                    "airflow",
                    "users",
                    "create",
                    "--username",
                    "airflow",
                    "--password",
                    "airflow",
                    "--firstname",
                    "admin",
                    "--lastname",
                    "admin",
                    "--role",
                    "Admin",
                    "--email",
                    "airflow@example.com",
                    # fmt: on
                ],
                env=environment,
            )

        # Sanity check that the plugin got loaded.
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
            metadata_file2=meta_file2,
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
    output_path: pathlib.Path,
    golden_path: pathlib.Path,
    ignore_paths: Sequence[str] = (),
) -> None:
    assert_metadata_files_equal(
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=ignore_paths,
        ignore_order=True,
    )


@dataclasses.dataclass
class DagTestCase:
    dag_id: str
    success: bool = True

    multiple_connections: bool = False
    platform_instance: Optional[str] = None
    enable_datajob_lineage: bool = True

    # used to identify the test case in the golden file when same DAG is used in multiple tests
    test_variant: Optional[str] = None

    @property
    def dag_test_id(self) -> str:
        return f"{self.dag_id}{self.test_variant or ''}"


test_cases = [
    DagTestCase(
        "simple_dag", multiple_connections=True, platform_instance=PLATFORM_INSTANCE
    ),
    DagTestCase(
        "simple_dag",
        multiple_connections=True,
        platform_instance=PLATFORM_INSTANCE,
        enable_datajob_lineage=False,
        test_variant="_no_datajob_lineage",
    ),
    DagTestCase("basic_iolets", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("dag_to_skip", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("snowflake_operator", success=False),
    DagTestCase("sqlite_operator", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("custom_operator_dag", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("custom_operator_sql_parsing"),
    DagTestCase("datahub_emitter_operator_jinja_template_dag"),
    DagTestCase("athena_operator"),
    DagTestCase("bigquery_insert_job_operator"),
]


@pytest.mark.parametrize(
    ["golden_filename", "test_case"],
    [
        *[
            pytest.param(
                (f"v2_{test_case.dag_test_id}"),
                test_case,
                id=(f"v2_{test_case.dag_test_id}"),
            )
            for test_case in test_cases
        ],
    ],
)
def test_airflow_plugin(
    tmp_path: pathlib.Path,
    golden_filename: str,
    test_case: DagTestCase,
) -> None:
    # This test:
    # - Configures the plugin.
    # - Starts a local airflow instance in a subprocess.
    # - Runs a DAG that uses an operator supported by the extractor.
    # - Waits for the DAG to complete.
    # - Validates the metadata generated against a golden file.

    golden_path = GOLDENS_FOLDER / f"{golden_filename}.json"
    dag_id = test_case.dag_id

    with _run_airflow(
        tmp_path,
        dags_folder=DAGS_FOLDER,
        multiple_connections=test_case.multiple_connections,
        platform_instance=test_case.platform_instance,
        enable_datajob_lineage=test_case.enable_datajob_lineage,
    ) as airflow_instance:
        print(f"Running DAG {dag_id}...")
        _wait_for_dag_to_load(airflow_instance, dag_id)
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

        try:
            _dump_dag_logs(airflow_instance, dag_id)
        except Exception as e:
            print(f"Failed to dump DAG logs: {e}")

    if dag_id == DAG_TO_SKIP_INGESTION:
        # Verify that no MCPs were generated.
        assert not os.path.exists(airflow_instance.metadata_file)
    else:
        _sanitize_output_file(airflow_instance.metadata_file)

        check_golden_file(
            output_path=airflow_instance.metadata_file,
            golden_path=golden_path,
            ignore_paths=[
                # TODO: If we switched to Git urls, maybe we could get this to work consistently.
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['datahub_sql_parser_error'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['openlineage_.*'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['log_url'\]",
                r"root\[\d+\]\['aspect'\]\['json'\]\['externalUrl'\]",
            ],
        )

        if test_case.multiple_connections:
            _sanitize_output_file(airflow_instance.metadata_file2)
            check_golden_file(
                output_path=airflow_instance.metadata_file2,
                golden_path=golden_path,
                ignore_paths=[
                    # TODO: If we switched to Git urls, maybe we could get this to work consistently.
                    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['datahub_sql_parser_error'\]",
                    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['openlineage_.*'\]",
                    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['log_url'\]",
                    r"root\[\d+\]\['aspect'\]\['json'\]\['externalUrl'\]",
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
        multiple_connections=False,
        platform_instance=None,
        enable_datajob_lineage=True,
    ) as airflow_instance:
        # input("Press enter to exit...")
        print("quitting airflow")
