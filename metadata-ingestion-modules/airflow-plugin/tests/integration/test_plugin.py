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

import packaging.version
import pytest
import requests
import tenacity
from airflow.models.connection import Connection

from datahub.ingestion.sink.file import write_metadata_file
from datahub.testing.compare_metadata_json import assert_metadata_files_equal
from datahub_airflow_plugin._airflow_shims import (
    AIRFLOW_VERSION,
    HAS_AIRFLOW_DAG_LISTENER_API,
    HAS_AIRFLOW_LISTENER_API,
    HAS_AIRFLOW_STANDALONE_CMD,
)

pytestmark = pytest.mark.integration

# Note: Airflow 3.0 tests on macOS may experience SIGSEGV crashes due to an upstream issue
# with gunicorn workers. This is a known Airflow bug (https://github.com/apache/airflow/issues/55838)
# and is not related to the DataHub plugin. The plugin code itself is fully Airflow 3.0 compatible.


def get_api_version() -> str:
    """Get the correct API version for the running Airflow version."""
    # Airflow 3.x uses v2 API endpoints only, v1 has been removed
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        return "v2"
    return "v1"


def is_airflow3() -> bool:
    """Check if the Airflow version is 3.0 or higher."""
    return AIRFLOW_VERSION >= packaging.version.parse("3.0.0")


def _make_api_request(
    session: requests.Session, url: str, timeout: int = 30
) -> requests.Response:
    """Make an API request with v2/v1 fallback for Airflow 3.0 compatibility issues."""
    try:
        res = session.get(url, timeout=timeout)
        res.raise_for_status()
        return res
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(
                f"[DEBUG] Authentication failed (401) for {url}. Session auth: {session.auth}"
            )
            print(f"[DEBUG] Response: {e.response.text[:200]}")
            raise
        elif e.response.status_code == 404 and "/api/v2/" in url:
            # Fallback to v1 if v2 is not available
            fallback_url = url.replace("/api/v2/", "/api/v1/")
            res = session.get(fallback_url, timeout=timeout)
            res.raise_for_status()
            return res
        else:
            raise


logger = logging.getLogger(__name__)
IS_LOCAL = os.environ.get("CI", "false") == "false"

# Base DAGs folder - specific folders are selected at runtime based on Airflow version
# This allows us to have different DAG implementations for operators that changed between versions
_BASE_DAGS_FOLDER = pathlib.Path(__file__).parent / "dags"
DAGS_FOLDER_AIRFLOW2 = _BASE_DAGS_FOLDER / "airflow2"
DAGS_FOLDER_AIRFLOW3 = _BASE_DAGS_FOLDER / "airflow3"

# For backward compatibility, keep DAGS_FOLDER pointing to the current environment's folder
# Note: This is only used when running the test script directly (if __name__ == "__main__")
if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
    DAGS_FOLDER = DAGS_FOLDER_AIRFLOW3
else:
    DAGS_FOLDER = DAGS_FOLDER_AIRFLOW2

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

        # Airflow 3.x uses JWT tokens for authentication
        if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
            # Get JWT token from login endpoint
            # The SimpleAuthManager token endpoint expects JSON
            login_url = f"{self.airflow_url}/auth/token"
            response = requests.post(
                login_url,
                json={"username": self.username, "password": self.password},
                timeout=10,
            )
            response.raise_for_status()
            token = response.json()["access_token"]
            session.headers["Authorization"] = f"Bearer {token}"
        else:
            # Airflow 2.x uses basic auth
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

    # Try the expected health endpoint for the version, with fallback
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        health_endpoints = ["/api/v2/monitor/health", "/health"]
    else:
        health_endpoints = ["/health"]

    res = None
    for endpoint in health_endpoints:
        try:
            res = requests.get(f"http://localhost:{airflow_port}{endpoint}", timeout=30)
            res.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404 and endpoint != health_endpoints[-1]:
                print(
                    f"[DEBUG] Health endpoint {endpoint} not found (404), trying next..."
                )
                continue
            else:
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
    api_version = get_api_version()
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/{api_version}/dags/{dag_id}/dagRuns",
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
    api_version = get_api_version()
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/{api_version}/dags",
    )

    if len(list(filter(lambda x: x["dag_id"] == dag_id, res.json()["dags"]))) == 0:
        raise NotReadyError("DAG was not loaded yet")


def _dump_dag_logs(airflow_instance: AirflowInstance, dag_id: str) -> None:
    # Get the dag run info
    api_version = get_api_version()
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/{api_version}/dags/{dag_id}/dagRuns",
    )
    dag_run = res.json()["dag_runs"][0]
    dag_run_id = dag_run["dag_run_id"]

    # List the tasks in the dag run
    res = _make_api_request(
        airflow_instance.session,
        f"{airflow_instance.airflow_url}/api/{api_version}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
    )
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
                f"{airflow_instance.airflow_url}/api/{api_version}/dags/{dag_id}/dagRuns/{dag_run_id}"
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
    is_v1: bool,
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

    # Get the Python executable path to ensure we use the correct environment
    python_executable = pathlib.Path(sys.executable)

    environment = {
        # Start with a clean environment to avoid interference from system-wide settings
        "PATH": str(python_executable.parent) + os.pathsep + os.environ.get("PATH", ""),
        "AIRFLOW_HOME": str(airflow_home),
        # Fix for macOS: Disable proxy detection to avoid SIGSEGV crashes after fork()
        # See: https://github.com/python/cpython/issues/58037
        "no_proxy": "*",
        # Enable Python fault handler for better SIGSEGV debugging
        "PYTHONFAULTHANDLER": "1",
        # Add debug wrapper for Airflow 3.0 to trace SIGSEGV crashes
        "PYTHONPATH": "/tmp"
        + (
            os.pathsep + os.environ.get("PYTHONPATH", "")
            if os.environ.get("PYTHONPATH")
            else ""
        ),
        # Airflow 3.x moved web_server_port to api section, but standalone might need both
        "AIRFLOW__API__PORT": str(airflow_port),
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": str(
            airflow_port
        ),  # Fallback for Airflow 2.x
        "AIRFLOW__WEBSERVER__BASE_URL": "http://airflow.example.com",
        "AIRFLOW__API__BASE_URL": "http://airflow.example.com",  # Airflow 3.0+ uses this for log URLs
        # Point airflow to the DAGs folder.
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(dags_folder),
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
        # Configure the datahub plugin and have it write the MCPs to a file.
        "AIRFLOW__CORE__LAZY_LOAD_PLUGINS": "False" if is_v1 else "True",
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
                "key_path": "/dev/null",
            },
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
            extra={
                "tmode": "ANSI",
            },
        ).get_uri(),
        # Ensure that the plugin waits for metadata to be written.
        # Note that we could also disable the RUN_IN_THREAD entirely,
        # but I want to minimize the difference between CI and prod.
        "DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT": "30",
        "DATAHUB_AIRFLOW_PLUGIN_USE_V1_PLUGIN": "true" if is_v1 else "false",
        # Convenience settings.
        "AIRFLOW__DATAHUB__LOG_LEVEL": "DEBUG",
        "AIRFLOW__DATAHUB__DEBUG_EMITTER": "True",
        "SQLALCHEMY_SILENCE_UBER_WARNING": "1",
        "AIRFLOW__DATAHUB__ENABLE_DATAJOB_LINEAGE": (
            "true" if enable_datajob_lineage else "false"
        ),
        # macOS: run setproctitle/gunicorn patch before any process forks.
        # Ensures the Airflow subprocess gets the patch even when the venv has no .pth (e.g. tox).
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
    }

    # Configure API authentication based on Airflow version
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        # Airflow 3.x: Use anonymous auth for testing to avoid JWT authentication issues
        environment["AIRFLOW__API__AUTH_BACKENDS"] = (
            "airflow.api.auth.backend.anonymous"
        )

        # Enable OpenLineage provider using ConsoleTransport (required for SQL parsing in Airflow 3.0)
        # ConsoleTransport logs events instead of sending them, avoiding network issues
        environment["AIRFLOW__OPENLINEAGE__TRANSPORT"] = '{"type": "console"}'

        # Use default disable_openlineage_plugin=true (DataHub-only mode)
        # OpenLineage plugin is disabled, but SQLParser is still patched for DataHub's use
        # This produces only DataHub's lowercase URNs, avoiding duplicates

        # Configure internal execution API JWT authentication
        # Required for executor to authenticate task execution requests
        environment["AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME"] = "300"  # 5 minutes
        environment["AIRFLOW__API_AUTH__JWT_SECRET"] = (
            "test-secret-key-for-jwt-signing-in-tests"
        )
        # Note: EXECUTION_API_SERVER_URL is set in airflow.cfg after db init to use correct port
    elif AIRFLOW_VERSION >= packaging.version.parse("2.10.0"):
        # Airflow 2.10+ with apache-airflow-providers-openlineage
        # Use basic auth for the API
        environment["AIRFLOW__API__AUTH_BACKEND"] = (
            "airflow.api.auth.backend.basic_auth"
        )

        # Enable OpenLineage provider using ConsoleTransport (required for SQL parsing)
        # This enables SQLParser calls even when DataHub is the primary lineage provider
        environment["AIRFLOW__OPENLINEAGE__TRANSPORT"] = '{"type": "console"}'

        # Use default disable_openlineage_plugin=true (DataHub-only mode)
        # OpenLineage plugin is disabled, but SQLParser is still patched for DataHub's use
        # This produces only DataHub's lowercase URNs, avoiding duplicates
    else:
        # Airflow 2.x < 2.10 supports basic auth for the API
        environment["AIRFLOW__API__AUTH_BACKEND"] = (
            "airflow.api.auth.backend.basic_auth"
        )

    # For Airflow 3.0+, configure for LocalExecutor (SequentialExecutor was removed)
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        # Airflow 3.0 uses LocalExecutor by default (SequentialExecutor was removed)
        pass

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

    if not HAS_AIRFLOW_STANDALONE_CMD:
        raise pytest.skip("Airflow standalone command is not available")

    # Find the airflow executable in the current Python environment
    # to ensure we use the correct version with the right dependencies
    airflow_executable = pathlib.Path(sys.executable).parent / "airflow"

    print(f"[DEBUG] Using Python: {sys.executable}")
    print(f"[DEBUG] Using Airflow: {airflow_executable}")
    print(
        f"[DEBUG] AIRFLOW__TRIGGERER__ENABLED = {environment.get('AIRFLOW__TRIGGERER__ENABLED', 'NOT SET')}"
    )

    # Verify greenlet version
    try:
        import greenlet

        print(f"[DEBUG] Greenlet version in test environment: {greenlet.__version__}")
    except ImportError:
        print("[DEBUG] Greenlet not installed in test environment")

    # Initialize the database before starting standalone (required for Airflow 3.x)
    print("[DEBUG] Initializing Airflow database...")
    subprocess.check_call(
        [str(airflow_executable), "db", "migrate"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # For Airflow 3.0, set execution API server URL to use the correct test port
    # This is needed because the default is localhost:8080, but we use a random port for testing
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        import configparser

        config_file = airflow_home / "airflow.cfg"
        if config_file.exists():
            config = configparser.ConfigParser()
            config.read(config_file)

            # Set execution API server URL in core section to point to the correct port
            execution_api_url = f"http://localhost:{airflow_port}/execution/"
            if "core" not in config:
                config.add_section("core")
            config.set("core", "execution_api_server_url", execution_api_url)

            with open(config_file, "w") as f:
                config.write(f)

            print(f"[DEBUG] Set execution_api_server_url = {execution_api_url}")

    # Use airflow standalone for both Airflow 2.x and 3.x
    # This starts all necessary components (scheduler, webserver/api-server, dag-processor)
    # Capture stdout/stderr to a log file for debugging
    logs_dir = airflow_home / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    standalone_log = logs_dir / "standalone.log"
    standalone_log_file = open(standalone_log, "w")

    print(f"[DEBUG] Starting airflow standalone, logging to {standalone_log}")

    # On macOS, PYTHONSTARTUP (set in environment above) loads _airflow_gunicorn_patch so
    # setproctitle is no-op'd and gunicorn does not call it after fork (avoids SIGSEGV).
    # See https://github.com/apache/airflow/issues/55838

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

        # Sleep for a few seconds to make sure the other Airflow processes are ready.
        time.sleep(3)

        # Check for DAG parsing errors in the log file
        if standalone_log.exists():
            log_content = standalone_log.read_text()
            if "Broken DAG" in log_content or "Failed to import" in log_content:
                print("[DEBUG] Found DAG parsing errors in standalone.log:")
                # Extract lines with errors
                error_lines = [
                    line
                    for line in log_content.split("\n")
                    if "Broken DAG" in line
                    or "Failed to import" in line
                    or "sqlite_operator" in line.lower()
                ]
                for line in error_lines[:10]:  # Show first 10 error lines
                    print(f"[DEBUG] {line}")

        # Create an extra "airflow" user for easy testing.
        # Note: In Airflow 3.0+ the users command is not available by default
        # The standalone command auto-generates an admin user which we'll use instead
        if IS_LOCAL:
            print("Creating an extra test user...")
            if not is_airflow3():
                subprocess.check_call(
                    [
                        # fmt: off
                        str(airflow_executable),
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
        if not is_v1:
            print("[debug] Listing loaded plugins")
            subprocess.check_call(
                [str(airflow_executable), "plugins", "-v"],
                env=environment,
            )

        # Load the admin user's password. This is generated by the
        # `airflow standalone` command, and is different from the
        # airflow user that we create when running locally.

        # In Airflow 3.0+, there are multiple possible password file locations
        password_files = [
            airflow_home / "standalone_admin_password.txt",  # Traditional location
            airflow_home
            / "simple_auth_manager_passwords.json.generated",  # New Airflow 3.0 location
        ]

        print(f"[DEBUG] Looking for password files in: {airflow_home}")
        print(f"[DEBUG] Airflow home exists: {airflow_home.exists()}")
        if airflow_home.exists():
            print(f"[DEBUG] Contents of airflow home: {list(airflow_home.iterdir())}")

        airflow_username = "admin"
        airflow_password = None
        password_source = None

        # Try to find password in any of the possible locations
        for password_file in password_files:
            if password_file.exists():
                print(f"[DEBUG] Found password file: {password_file.name}")
                try:
                    if password_file.name.endswith(".json.generated"):
                        # Handle JSON format for simple_auth_manager_passwords.json.generated
                        import json

                        content = json.loads(password_file.read_text())
                        # The structure might be {"admin": "password"} or similar
                        if "admin" in content:
                            airflow_password = content["admin"]
                            password_source = password_file.name
                        else:
                            print(f"[DEBUG] JSON file structure: {content}")
                            # Try to get first password value if admin key not found
                            for key, value in content.items():
                                if (
                                    isinstance(value, str) and len(value) > 5
                                ):  # Looks like a password
                                    airflow_password = value
                                    password_source = (
                                        f"{password_file.name} (key: {key})"
                                    )
                                    break
                    else:
                        # Handle plain text format for standalone_admin_password.txt
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

        # If no password found, wait and try again
        if not airflow_password:
            print("[DEBUG] No password files found, waiting for file creation...")
            time.sleep(2)
            for password_file in password_files:
                if password_file.exists():
                    try:
                        if password_file.name.endswith(".json.generated"):
                            import json

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

        # Final fallback
        if not airflow_password:
            print(
                "[DEBUG] No password files found after waiting, using fallback credentials"
            )
            airflow_password = "admin"  # Try default admin password

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
        # Shutdown all Airflow processes
        print("Shutting down airflow...")
        for proc in airflow_processes:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception as e:
                print(f"Error sending SIGINT to process {proc.pid}: {e}")

        # Wait for graceful shutdown
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

    v2_only: bool = False
    multiple_connections: bool = False
    platform_instance: Optional[str] = None
    enable_datajob_lineage: bool = True
    cluster: Optional[str] = None

    # used to identify the test case in the golden file when same DAG is used in multiple tests
    test_variant: Optional[str] = None

    @property
    def dag_test_id(self) -> str:
        return f"{self.dag_id}{self.test_variant or ''}"


# Airflow 2.x test cases - these DAGs are in tests/integration/dags/
test_cases_airflow2 = [
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
    DagTestCase(
        "airflow_asset_iolets", v2_only=True, platform_instance=PLATFORM_INSTANCE
    ),
    DagTestCase("dag_to_skip", v2_only=True, platform_instance=PLATFORM_INSTANCE),
    DagTestCase("snowflake_operator", success=False, v2_only=True),
    DagTestCase("sqlite_operator", v2_only=True, platform_instance=PLATFORM_INSTANCE),
    DagTestCase(
        "custom_operator_dag", v2_only=True, platform_instance=PLATFORM_INSTANCE
    ),
    DagTestCase("custom_operator_sql_parsing", v2_only=True),
    DagTestCase("datahub_emitter_operator_jinja_template_dag", v2_only=True),
    DagTestCase("athena_operator", v2_only=True),
    DagTestCase("bigquery_insert_job_operator", v2_only=True),
    DagTestCase(
        "bigquery_insert_job_operator",
        v2_only=True,
        cluster="DEV",
        test_variant="_dev_cluster",
    ),
    DagTestCase("teradata_operator", v2_only=True),
]

# Airflow 3.x test cases - these DAGs are in tests/integration/dags/airflow3/
# All test cases are v2_only since Airflow 3.0+ only supports the listener-based plugin
test_cases_airflow3 = [
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
    # @asset decorated DAGs - Airflow 3.0+ feature
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


def _get_test_parameters():
    """Generate test parameters based on the Airflow version."""
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        # Airflow 3.0+: Only run v2 plugin tests with airflow3 suffix
        return [
            pytest.param(
                f"v2_{test_case.dag_test_id}_airflow3",
                test_case,
                False,  # is_v1
                id=f"v2_{test_case.dag_test_id}_airflow3",
            )
            for test_case in test_cases_airflow3
        ]
    else:
        # Airflow 2.x: Run both v1 and v2 plugin tests
        return [
            # v1 plugin tests (only on Airflow 2.3)
            *[
                pytest.param(
                    f"v1_{test_case.dag_test_id}",
                    test_case,
                    True,
                    id=f"v1_{test_case.dag_test_id}",
                    marks=pytest.mark.skipif(
                        AIRFLOW_VERSION >= packaging.version.parse("2.4.0"),
                        reason="We only test the v1 plugin on Airflow 2.3",
                    ),
                )
                for test_case in test_cases_airflow2
                if not test_case.v2_only
            ],
            # v2 plugin tests
            *[
                pytest.param(
                    (
                        f"v2_{test_case.dag_test_id}"
                        if HAS_AIRFLOW_DAG_LISTENER_API
                        else f"v2_{test_case.dag_test_id}_no_dag_listener"
                    ),
                    test_case,
                    False,
                    id=(
                        f"v2_{test_case.dag_test_id}"
                        if HAS_AIRFLOW_DAG_LISTENER_API
                        else f"v2_{test_case.dag_test_id}_no_dag_listener"
                    ),
                    marks=[
                        pytest.mark.skipif(
                            not HAS_AIRFLOW_LISTENER_API,
                            reason="Cannot test plugin v2 without the Airflow plugin listener API",
                        ),
                        pytest.mark.skipif(
                            AIRFLOW_VERSION < packaging.version.parse("2.4.0"),
                            reason="We skip testing the v2 plugin on Airflow 2.3 because it causes flakiness in the custom properties. "
                            "Ideally we'd just fix these, but given that Airflow 2.3 is EOL and likely going to be deprecated "
                            "soon anyways, it's not worth the effort.",
                        ),
                    ],
                )
                for test_case in test_cases_airflow2
            ],
        ]


@pytest.mark.parametrize(
    ["golden_filename", "test_case", "is_v1"],
    _get_test_parameters(),
)
def test_airflow_plugin(
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

    # Support provider-specific golden files for apache-airflow-providers-openlineage tests
    # When using the provider package (Airflow 2.10+), OpenLineage version differs from
    # legacy openlineage-airflow package, causing cosmetic metadata formatting differences.
    # Tests without provider-specific golden files are skipped in provider environments.
    # Note: This only applies to Airflow 2.x - Airflow 3.x always uses the provider and
    # has separate _airflow3.json golden files.
    golden_path = GOLDENS_FOLDER / f"{golden_filename}.json"
    if not is_airflow3():
        try:
            from datahub_airflow_plugin.airflow2._openlineage_compat import (
                USE_OPENLINEAGE_PROVIDER,
            )

            if USE_OPENLINEAGE_PROVIDER:
                provider_golden_path = (
                    GOLDENS_FOLDER / f"{golden_filename}_provider.json"
                )
                if provider_golden_path.exists():
                    golden_path = provider_golden_path
                else:
                    pytest.skip(
                        f"Skipping test in provider environment: no {golden_filename}_provider.json golden file exists. "
                        "This test only runs with the standalone openlineage-airflow package."
                    )
        except ImportError:
            pass

    dag_id = test_case.dag_id

    # Select the appropriate DAGs folder based on the Airflow version being tested
    # For Airflow 3.0+ tests, use the airflow3 subfolder
    # For Airflow 2.x tests, use the airflow2 subfolder
    if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
        dags_folder = DAGS_FOLDER_AIRFLOW3
    else:
        dags_folder = DAGS_FOLDER_AIRFLOW2

    with _run_airflow(
        tmp_path,
        dags_folder=dags_folder,
        is_v1=is_v1,
        multiple_connections=test_case.multiple_connections,
        platform_instance=test_case.platform_instance,
        enable_datajob_lineage=test_case.enable_datajob_lineage,
        cluster=test_case.cluster,
    ) as airflow_instance:
        print(f"Running DAG {dag_id}...")
        _wait_for_dag_to_load(airflow_instance, dag_id)

        # Build trigger command with version-appropriate date parameter
        trigger_cmd = [
            str(airflow_instance.airflow_executable),
            "dags",
            "trigger",
        ]

        # Airflow 3.x uses --logical-date, 2.x uses --exec-date
        if AIRFLOW_VERSION >= packaging.version.parse("3.0.0"):
            trigger_cmd.extend(["--logical-date", "2023-09-27T21:34:38+00:00"])
        else:
            trigger_cmd.extend(["--exec-date", "2023-09-27T21:34:38+00:00"])

        trigger_cmd.extend(["-r", "manual_run_test", dag_id])

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
        is_v1=not HAS_AIRFLOW_LISTENER_API,
        multiple_connections=False,
        platform_instance=None,
        enable_datajob_lineage=True,
        cluster=None,
    ) as airflow_instance:
        # input("Press enter to exit...")
        print("quitting airflow")
