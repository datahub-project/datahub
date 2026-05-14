import contextlib
import dataclasses
import functools
import json
import logging
import os
import pathlib
import platform
import random
import signal
import subprocess
import sys
import textwrap
import time
from typing import Any, Iterator, Optional, Sequence

import pytest
import requests
import tenacity
from airflow.sdk import Connection

from datahub.ingestion.sink.file import write_metadata_file
from datahub.testing.compare_metadata_json import assert_metadata_files_equal

pytestmark = pytest.mark.integration

# Note: Airflow 3.0 tests on macOS may experience SIGSEGV crashes due to an upstream issue
# with gunicorn workers. See https://github.com/apache/airflow/issues/55838 — unrelated to
# the DataHub plugin.


def _make_api_request(
    session: requests.Session, url: str, timeout: int = 30
) -> requests.Response:
    res = session.get(url, timeout=timeout)
    res.raise_for_status()
    return res


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
    airflow_executable: pathlib.Path

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
        # Airflow 3.x uses JWT tokens issued by SimpleAuthManager.
        login_url = f"{self.airflow_url}/auth/token"
        response = requests.post(
            login_url,
            json={"username": self.username, "password": self.password},
            timeout=10,
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        session.headers["Authorization"] = f"Bearer {token}"
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

    # Try Airflow 3 endpoint first, fall back to /health if 404.
    health_endpoints = ["/api/v2/monitor/health", "/health"]
    res = None
    for endpoint in health_endpoints:
        try:
            res = requests.get(f"http://localhost:{airflow_port}{endpoint}", timeout=30)
            res.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404 and endpoint != health_endpoints[-1]:
                continue
            raise

    if res is None:
        raise RuntimeError("No working health endpoint found")

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
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/v2/dags/{dag_id}/dagRuns",
    )

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
    stop=tenacity.stop_after_delay(180),
    retry=tenacity.retry_if_exception_type(NotReadyError),
)
def _wait_for_dag_to_load(airflow_instance: AirflowInstance, dag_id: str) -> None:
    print(f"Checking if DAG {dag_id} was loaded")
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/v2/dags",
    )

    if len(list(filter(lambda x: x["dag_id"] == dag_id, res.json()["dags"]))) == 0:
        raise NotReadyError("DAG was not loaded yet")


def _dump_dag_logs(airflow_instance: AirflowInstance, dag_id: str) -> None:
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/v2/dags/{dag_id}/dagRuns",
    )
    dag_run = res.json()["dag_runs"][0]
    dag_run_id = dag_run["dag_run_id"]

    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
    )
    task_instances = res.json()["task_instances"]
    task_instances.sort(key=lambda x: x["start_date"] or "")

    print(f"\nTask execution order for DAG {dag_id}:")
    for task in task_instances:
        task_id = task["task_id"]
        state = task["state"]
        try_number = task.get("try_number", 1)

        task_header = f"Task: {task_id} (State: {state}; Try: {try_number})"

        try:
            res = airflow_instance.session.get(
                f"{airflow_instance.airflow_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
                f"/taskInstances/{task_id}/logs/{try_number}",
                params={"full_content": "true"},
                timeout=5,
            )
            res.raise_for_status()
            print(f"\n=== {task_header} ===\n{textwrap.indent(res.text, '    ')}")
        except Exception as e:
            print(f"Failed to fetch logs for {task_header}: {e}")


@contextlib.contextmanager
def _run_airflow(  # noqa: C901 - Test helper function with necessary complexity
    tmp_path: pathlib.Path,
    dags_folder: pathlib.Path,
    multiple_connections: bool,
    platform_instance: Optional[str],
    enable_datajob_lineage: bool,
    cluster: Optional[str] = None,
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

    python_executable = pathlib.Path(sys.executable)

    environment = {
        "PATH": str(python_executable.parent) + os.pathsep + os.environ.get("PATH", ""),
        "AIRFLOW_HOME": str(airflow_home),
        # macOS: disable proxy detection to avoid SIGSEGV crashes after fork().
        # See https://github.com/python/cpython/issues/58037.
        "no_proxy": "*",
        "PYTHONFAULTHANDLER": "1",
        "PYTHONPATH": "/tmp"
        + (
            os.pathsep + os.environ.get("PYTHONPATH", "")
            if os.environ.get("PYTHONPATH")
            else ""
        ),
        "AIRFLOW__API__PORT": str(airflow_port),
        "AIRFLOW__API__BASE_URL": "http://airflow.example.com",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_folder),
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
        "AIRFLOW__CORE__LAZY_LOAD_PLUGINS": "True",
        "AIRFLOW__DATAHUB__CONN_ID": (
            f"{datahub_connection_name}, {datahub_connection_name_2}"
            if multiple_connections
            else datahub_connection_name
        ),
        "AIRFLOW__DATAHUB__DAG_FILTER_STR": f'{{ "deny": ["{DAG_TO_SKIP_INGESTION}"] }}',
        f"AIRFLOW_CONN_{datahub_connection_name.upper()}": Connection(
            conn_id="datahub_file_default",
            conn_type="datahub-file",
            host=str(meta_file),
        ).get_uri(),
        "AIRFLOW_CONN_MY_SNOWFLAKE": Connection(
            conn_id="my_snowflake",
            conn_type="snowflake",
            login="fake_username",
            password="fake_password",
            schema="DATAHUB_TEST_SCHEMA",
            extra=json.dumps(
                {
                    "account": "fake_account",
                    "database": "DATAHUB_TEST_DATABASE",
                    "warehouse": "fake_warehouse",
                    "role": "fake_role",
                    "insecure_mode": "true",
                }
            ),
        ).get_uri(),
        "AIRFLOW_CONN_MY_AWS": Connection(
            conn_id="my_aws",
            conn_type="aws",
            extra=json.dumps(
                {
                    "region_name": "us-east-1",
                    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                }
            ),
        ).get_uri(),
        "AIRFLOW_CONN_MY_BIGQUERY": Connection(
            conn_id="my_bigquery",
            conn_type="google_cloud_platform",
            extra=json.dumps({"project": "test_project", "key_path": "/dev/null"}),
        ).get_uri(),
        "AIRFLOW_CONN_MY_SQLITE": Connection(
            conn_id="my_sqlite",
            conn_type="sqlite",
            host=str(tmp_path / "my_sqlite.db"),
        ).get_uri(),
        "AIRFLOW_CONN_MY_TERADATA": Connection(
            conn_id="my_teradata",
            conn_type="teradata",
            host="fake_teradata_host",
            login="fake_username",
            password="fake_password",
            extra=json.dumps({"tmode": "ANSI"}),
        ).get_uri(),
        # Ensure the plugin waits for metadata to be written before the task exits.
        # We could disable RUN_IN_THREAD entirely, but we want CI to mirror prod
        # (where the listener runs in a thread).
        "DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT": "30",
        "AIRFLOW__DATAHUB__LOG_LEVEL": "DEBUG",
        "AIRFLOW__DATAHUB__DEBUG_EMITTER": "True",
        "SQLALCHEMY_SILENCE_UBER_WARNING": "1",
        "AIRFLOW__DATAHUB__ENABLE_DATAJOB_LINEAGE": (
            "true" if enable_datajob_lineage else "false"
        ),
        # macOS: run the setproctitle/gunicorn patch before any process forks so the
        # Airflow subprocess inherits it even when the venv has no .pth (e.g. under tox).
        **(
            {
                "PYTHONSTARTUP": str(
                    pathlib.Path(__file__).resolve().parent
                    / "_airflow_gunicorn_patch.py"
                ),
            }
            if platform.system() == "Darwin"
            else {}
        ),
        # Airflow 3.x: anonymous auth for tests.
        "AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.anonymous",
        # OpenLineage ConsoleTransport so SQLParser fires without sending events anywhere.
        "AIRFLOW__OPENLINEAGE__TRANSPORT": '{"type": "console"}',
        # Internal execution API JWT settings.
        "AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME": "300",
        "AIRFLOW__API_AUTH__JWT_SECRET": "test-secret-key-for-jwt-signing-in-tests",
    }

    if platform_instance:
        environment["AIRFLOW__DATAHUB__PLATFORM_INSTANCE"] = platform_instance

    if cluster:
        environment["AIRFLOW__DATAHUB__CLUSTER"] = cluster

    if multiple_connections:
        environment[f"AIRFLOW_CONN_{datahub_connection_name_2.upper()}"] = Connection(
            conn_id="datahub_file_default2",
            conn_type="datahub-file",
            host=str(meta_file2),
        ).get_uri()

    airflow_executable = pathlib.Path(sys.executable).parent / "airflow"

    print(f"[DEBUG] Using Python: {sys.executable}")
    print(f"[DEBUG] Using Airflow: {airflow_executable}")

    try:
        import greenlet

        print(f"[DEBUG] Greenlet version in test environment: {greenlet.__version__}")
    except ImportError:
        print("[DEBUG] Greenlet not installed in test environment")

    print("[DEBUG] Initializing Airflow database...")
    subprocess.check_call(
        [str(airflow_executable), "db", "migrate"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # execution_api_server_url must point at the random test port.
    import configparser

    config_file = airflow_home / "airflow.cfg"
    if config_file.exists():
        config = configparser.ConfigParser()
        config.read(config_file)

        execution_api_url = f"http://localhost:{airflow_port}/execution/"
        if "core" not in config:
            config.add_section("core")
        config.set("core", "execution_api_server_url", execution_api_url)

        with open(config_file, "w") as f:
            config.write(f)

        print(f"[DEBUG] Set execution_api_server_url = {execution_api_url}")

    logs_dir = airflow_home / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    standalone_log = logs_dir / "standalone.log"
    standalone_log_file = open(standalone_log, "w")

    print(f"[DEBUG] Starting airflow standalone, logging to {standalone_log}")

    airflow_process = subprocess.Popen(
        [str(airflow_executable), "standalone"],
        env=environment,
        stdout=standalone_log_file,
        stderr=subprocess.STDOUT,
    )
    airflow_processes = [airflow_process]

    try:
        _wait_for_airflow_healthy(airflow_port)
        print("Airflow is ready!")

        time.sleep(3)

        if standalone_log.exists():
            log_content = standalone_log.read_text()
            if "Broken DAG" in log_content or "Failed to import" in log_content:
                print("[DEBUG] Found DAG parsing errors in standalone.log:")
                error_lines = [
                    line
                    for line in log_content.split("\n")
                    if "Broken DAG" in line
                    or "Failed to import" in line
                    or "sqlite_operator" in line.lower()
                ]
                for line in error_lines[:10]:
                    print(f"[DEBUG] {line}")

        print("[debug] Listing loaded plugins")
        subprocess.check_call(
            [str(airflow_executable), "plugins", "-v"],
            env=environment,
        )

        # Load the admin user's password. Standalone auto-generates one in Airflow 3.
        password_files = [
            airflow_home / "standalone_admin_password.txt",
            airflow_home / "simple_auth_manager_passwords.json.generated",
        ]

        print(f"[DEBUG] Looking for password files in: {airflow_home}")
        if airflow_home.exists():
            print(f"[DEBUG] Contents of airflow home: {list(airflow_home.iterdir())}")

        airflow_username = "admin"
        airflow_password = None
        password_source = None

        for password_file in password_files:
            if password_file.exists():
                print(f"[DEBUG] Found password file: {password_file.name}")
                try:
                    if password_file.name.endswith(".json.generated"):
                        content = json.loads(password_file.read_text())
                        if "admin" in content:
                            airflow_password = content["admin"]
                            password_source = password_file.name
                        else:
                            print(f"[DEBUG] JSON file structure: {content}")
                            for key, value in content.items():
                                if isinstance(value, str) and len(value) > 5:
                                    airflow_password = value
                                    password_source = (
                                        f"{password_file.name} (key: {key})"
                                    )
                                    break
                    else:
                        airflow_password = password_file.read_text().strip()
                        password_source = password_file.name

                    if airflow_password:
                        print(
                            f"[DEBUG] Using admin credentials from {password_source}, password_length={len(airflow_password)}"
                        )
                        break
                except Exception as e:
                    print(f"[DEBUG] Error reading {password_file.name}: {e}")
                    continue

        if not airflow_password:
            print("[DEBUG] No password files found, waiting for file creation...")
            time.sleep(2)
            for password_file in password_files:
                if password_file.exists():
                    try:
                        if password_file.name.endswith(".json.generated"):
                            content = json.loads(password_file.read_text())
                            if "admin" in content:
                                airflow_password = content["admin"]
                                password_source = password_file.name
                            else:
                                for key, value in content.items():
                                    if isinstance(value, str) and len(value) > 5:
                                        airflow_password = value
                                        password_source = (
                                            f"{password_file.name} (key: {key})"
                                        )
                                        break
                        else:
                            airflow_password = password_file.read_text().strip()
                            password_source = password_file.name

                        if airflow_password:
                            print(
                                f"[DEBUG] Found admin credentials after waiting: {password_source}, password_length={len(airflow_password)}"
                            )
                            break
                    except Exception as e:
                        print(
                            f"[DEBUG] Error reading {password_file.name} after wait: {e}"
                        )
                        continue

        if not airflow_password:
            print(
                "[DEBUG] No password files found after waiting, using fallback credentials"
            )
            airflow_password = "admin"

        print(
            f"[DEBUG] Final credentials: username={airflow_username}, password_length={len(airflow_password)}"
        )

        airflow_instance = AirflowInstance(
            airflow_home=airflow_home,
            airflow_port=airflow_port,
            pid=airflow_process.pid,
            env_vars=environment,
            airflow_executable=airflow_executable,
            username=airflow_username,
            password=airflow_password,
            metadata_file=meta_file,
            metadata_file2=meta_file2,
        )

        yield airflow_instance
    finally:
        print("Shutting down airflow...")
        for proc in airflow_processes:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception as e:
                print(f"Error sending SIGINT to process {proc.pid}: {e}")

        for proc in airflow_processes:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(f"Hard shutting down process {proc.pid}...")
                try:
                    proc.kill()
                    proc.wait(timeout=3)
                except Exception as e:
                    print(f"Error killing process {proc.pid}: {e}")


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
    cluster: Optional[str] = None

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
    DagTestCase("airflow_asset_iolets", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("decorated_asset_producer", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("decorated_asset_with_file", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("consume_decorated_assets", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("dag_to_skip", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("snowflake_operator", success=False),
    DagTestCase("sqlite_operator", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("custom_operator_dag", platform_instance=PLATFORM_INSTANCE),
    DagTestCase("custom_operator_sql_parsing"),
    DagTestCase("datahub_emitter_operator_jinja_template_dag"),
    DagTestCase("athena_operator"),
    DagTestCase("bigquery_insert_job_operator"),
    DagTestCase(
        "bigquery_insert_job_operator", cluster="DEV", test_variant="_dev_cluster"
    ),
    DagTestCase("teradata_operator"),
    DagTestCase("athena_operator", cluster="DEV", test_variant="_dev_cluster"),
    DagTestCase("teradata_operator", cluster="DEV", test_variant="_dev_cluster"),
    DagTestCase(
        "snowflake_operator", cluster="DEV", test_variant="_dev_cluster", success=False
    ),
]


@pytest.mark.parametrize(
    ["golden_filename", "test_case"],
    [
        pytest.param(
            f"v2_{test_case.dag_test_id}",
            test_case,
            id=f"v2_{test_case.dag_test_id}",
        )
        for test_case in test_cases
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
        cluster=test_case.cluster,
    ) as airflow_instance:
        print(f"Running DAG {dag_id}...")
        _wait_for_dag_to_load(airflow_instance, dag_id)

        trigger_cmd = [
            str(airflow_instance.airflow_executable),
            "dags",
            "trigger",
            "--logical-date",
            "2023-09-27T21:34:38+00:00",
            "-r",
            "manual_run_test",
            dag_id,
        ]

        subprocess.check_call(
            trigger_cmd,
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
        cluster=None,
    ) as airflow_instance:
        # input("Press enter to exit...")
        print("quitting airflow")
