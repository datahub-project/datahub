import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import pytest
import requests
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2023-10-15 07:00:00"
# We'll check and dynamically determine the working API port
AIRBYTE_API_HOST = "localhost"
AIRBYTE_WEB_PORT = 8000
AIRBYTE_API_PORT = 8001
BASIC_AUTH_USERNAME = "airbyte"
BASIC_AUTH_PASSWORD = "password"
POSTGRES_PORT = 5433
MYSQL_PORT = 30306
# Define this global variable at module level to fix "name-defined" errors
AIRBYTE_API_URL: str = f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1"


def is_postgres_ready(container_name: str) -> bool:
    """Check if PostgreSQL is responsive on a container"""
    try:
        cmd = f"docker exec {container_name} pg_isready -U test"
        ret = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
        return ret.returncode == 0
    except subprocess.SubprocessError:
        return False


def is_mysql_ready(container_name: str) -> bool:
    """Check if MySQL is responsive on a container"""
    try:
        cmd = f"docker exec {container_name} mysqladmin ping -h localhost -u root -prootpwd123"
        ret = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
        return ret.returncode == 0
    except subprocess.SubprocessError:
        return False


def check_container_logs_for_errors(
    container_name: str, error_patterns: Optional[List[str]] = None
) -> bool:
    """Check if container logs contain specific error patterns"""
    if error_patterns is None:
        error_patterns = ["error", "exception", "failed"]

    try:
        logs = subprocess.check_output(
            f"docker logs {container_name} 2>&1 | grep -i -E '({'|'.join(error_patterns)})' | tail -20",
            shell=True,
            text=True,
        )
        if logs.strip():
            print(f"Found errors in {container_name} logs:")
            print(logs)
            return True
        return False
    except subprocess.CalledProcessError:
        # grep returns non-zero if no matches found, which is good in this case
        return False
    except Exception as e:
        print(f"Error checking logs for {container_name}: {str(e)}")
        return False


def check_container_health(container_name: str) -> Optional[str]:
    """Check container health status if available"""
    try:
        health = subprocess.check_output(
            f"docker inspect --format='{{{{.State.Health.Status}}}}' {container_name}",
            shell=True,
            text=True,
        ).strip()
        if health:
            print(f"{container_name} health status: {health}")
            return health
        return None
    except Exception:
        return None


def find_working_api_url() -> Optional[Dict[str, Any]]:
    """Determine which port and path combination works for the Airbyte API"""
    # Different endpoint paths to try
    api_paths = [
        "/api/v1/health",
        "/api/public/v1/health",
        "/api/public/health",
        "/health",
    ]

    # Try both common ports
    for port in [AIRBYTE_API_PORT, AIRBYTE_WEB_PORT]:
        for path in api_paths:
            url = f"http://{AIRBYTE_API_HOST}:{port}{path}"
            try:
                # Try without auth first
                response = requests.get(url, timeout=5)
                auth_type = "none"

                # If we get 401, try with auth
                if response.status_code == 401:
                    response = requests.get(
                        url, auth=(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD), timeout=5
                    )
                    auth_type = "basic"

                print(f"Checking {url} - Status: {response.status_code}")

                # If we get a good response, return this configuration
                if response.status_code == 200 and (
                    "available" in response.text.lower()
                    or "ok" in response.text.lower()
                ):
                    print(f"✅ Found working API URL: {url} (auth: {auth_type})")
                    return {
                        "url": url,
                        "port": port,
                        "path": path,
                        "auth_required": auth_type == "basic",
                    }
            except requests.RequestException as e:
                print(f"Error checking {url}: {type(e).__name__}")

    return None


def diagnose_airbyte_proxy_issue() -> None:
    """Run diagnostics for proxy 502 Bad Gateway issues"""
    print("\n🔍 DIAGNOSING AIRBYTE PROXY ISSUE")

    # Check if relevant containers are running
    containers = ["airbyte-proxy", "airbyte-server", "airbyte-db", "airbyte-webapp"]
    for container in containers:
        try:
            status = subprocess.check_output(
                f"docker ps --filter name={container} --format '{{{{.Status}}}}'",
                shell=True,
                text=True,
            ).strip()
            if status:
                print(f"{container}: RUNNING - {status}")
                health = check_container_health(container)
                if health and health != "healthy":
                    print(f"⚠️ {container} health is {health}")
            else:
                print(f"❌ {container}: NOT RUNNING")
        except Exception as e:
            print(f"Error checking {container}: {str(e)}")

    # Check network connectivity between containers
    try:
        print("\nChecking network connectivity between containers:")
        subprocess.run(
            "docker exec airbyte-proxy ping -c 2 airbyte-server", shell=True, timeout=5
        )
    except Exception as e:
        print(f"Network connectivity check failed: {str(e)}")

    # Check for port conflicts on host
    for port in [AIRBYTE_WEB_PORT, AIRBYTE_API_PORT]:
        try:
            output = subprocess.check_output(
                f"netstat -tuln | grep {port}", shell=True, text=True
            ).strip()
            if output:
                print(f"Port {port} usage:\n{output}")
        except Exception:
            print(f"No conflicts detected on port {port}")

    # Check logs for specific errors
    print("\nChecking for errors in container logs:")
    for container in containers:
        check_container_logs_for_errors(container)

    print("\nTrying to send a direct request to the server container:")
    try:
        # Get the internal IP of the server container
        ip = subprocess.check_output(
            "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airbyte-server",
            shell=True,
            text=True,
        ).strip()
        if ip:
            print(f"airbyte-server IP: {ip}")
            # Try direct request to server container
            subprocess.run(
                f"docker exec airbyte-proxy curl -v http://{ip}:8001/api/v1/health",
                shell=True,
                timeout=10,
            )
    except Exception as e:
        print(f"Error with direct server request: {str(e)}")

    print("\n🔍 END OF DIAGNOSTICS\n")


def get_api_url_for_test() -> Tuple[str, bool]:
    """Determine the appropriate API URL for testing, with fallbacks"""
    # Try to find a working API URL
    api_config = find_working_api_url()

    if api_config:
        # If we found a working URL, use it
        if api_config["auth_required"]:
            return (f"http://{AIRBYTE_API_HOST}:{api_config['port']}/api/v1", True)
        else:
            return (f"http://{AIRBYTE_API_HOST}:{api_config['port']}/api/v1", False)
    else:
        # Run diagnostics if we couldn't find a working URL
        diagnose_airbyte_proxy_issue()

        # Fall back to the standard URL and assume auth is required
        print("⚠️ Could not find working API URL, using default with auth")
        return (f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1", True)


def wait_for_airbyte_ready(timeout: int = 600) -> bool:
    """Wait for Airbyte API to be ready with better diagnostics"""
    print(f"Waiting for Airbyte services to be ready (timeout: {timeout}s)...")

    # Check container status periodically
    end_time = time.time() + timeout
    check_interval = 10  # seconds
    attempt = 0

    while time.time() < end_time:
        attempt += 1
        print(f"\nAttempt {attempt}: Checking Airbyte container status...")

        containers = [
            "airbyte-db",
            "airbyte-temporal",
            "airbyte-server",
            "airbyte-worker",
            "airbyte-proxy",
        ]

        for container in containers:
            try:
                status = subprocess.check_output(
                    f"docker ps --filter name={container} --format '{{{{.Status}}}}'",
                    shell=True,
                    text=True,
                ).strip()
                if status:
                    health = check_container_health(container)
                    if health:
                        status_msg = f"RUNNING - {status} (Health: {health})"
                    else:
                        status_msg = f"RUNNING - {status}"
                    print(f"{container}: {status_msg}")

                    if health and health not in ["healthy", "starting"]:
                        print(f"⚠️ {container} is in {health} state")
                else:
                    print(f"❌ {container}: NOT RUNNING")
            except Exception as e:
                print(f"Error checking {container}: {str(e)}")

        # Try to determine a working API URL
        global AIRBYTE_API_URL
        api_url, needs_auth = get_api_url_for_test()
        AIRBYTE_API_URL = api_url
        print(f"Testing API URL: {api_url} (Auth: {needs_auth})")

        try:
            # Try to get a workspaces list as a deeper check
            auth = (BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD) if needs_auth else None

            workspaces_response = requests.post(
                f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
            )

            if workspaces_response.status_code == 200:
                print(
                    "✅ Successfully connected to Airbyte API and retrieved workspaces!"
                )
                return True
            else:
                print(
                    f"❌ API request failed: {workspaces_response.status_code} - {workspaces_response.text[:100]}"
                )
        except requests.RequestException as e:
            print(f"❌ API connection error: {type(e).__name__}: {str(e)}")

        # If we're halfway through the timeout and still not ready, check logs for errors
        if attempt % 3 == 0 or time.time() > (end_time - timeout / 2):
            check_logs = ["airbyte-server", "airbyte-proxy", "airbyte-db"]
            for container in check_logs:
                print(f"\nChecking recent logs for {container}:")
                try:
                    logs = subprocess.check_output(
                        f"docker logs {container} --tail 10", shell=True, text=True
                    )
                    print(logs)
                except Exception:
                    print(f"Could not retrieve logs for {container}")

        # Wait before next check
        print(f"Waiting {check_interval} seconds before next check...")
        time.sleep(check_interval)

    # If we got here, we timed out
    print("⛔ Timeout waiting for Airbyte services!")
    diagnose_airbyte_proxy_issue()
    raise TimeoutError(f"Airbyte API did not become ready within {timeout} seconds")


def setup_airbyte_connections(test_resources_dir: Path) -> None:
    """Set up Airbyte connections using the setup script with better error handling"""
    # Ensure the setup script is executable
    setup_script = test_resources_dir / "setup/setup_airbyte.sh"
    subprocess.run(["chmod", "+x", str(setup_script)], check=True)

    # Modify the script to use the correct API URL if we found a different one
    if f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1" != AIRBYTE_API_URL:
        print(f"Modifying setup script to use API URL: {AIRBYTE_API_URL}")
        try:
            # Create a temporary copy of the script with the modified URL
            with open(setup_script, "r") as f:
                script_content = f.read()

            # Replace the API URL
            script_content = script_content.replace(
                f'AIRBYTE_API="http://localhost:{AIRBYTE_API_PORT}/api/v1"',
                f'AIRBYTE_API="{AIRBYTE_API_URL}"',
            )

            # Save to a temporary file
            temp_script = test_resources_dir / "setup_airbyte_modified.sh"
            with open(temp_script, "w") as f:
                f.write(script_content)

            # Make it executable
            subprocess.run(["chmod", "+x", str(temp_script)], check=True)

            # Use the temporary script
            setup_script = temp_script
        except Exception as e:
            print(f"Failed to modify setup script: {str(e)}")
            print("Using original script")

    # Run the setup script
    print("Running Airbyte setup script...")
    try:
        result = subprocess.run(
            [str(setup_script)], check=False, capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"⚠️ Setup script failed with exit code {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

            # Try to run key setup commands directly if the script fails
            try_direct_api_setup()
        else:
            print("✅ Setup script completed successfully")
    except Exception as e:
        print(f"⚠️ Error running setup script: {str(e)}")
        try_direct_api_setup()


def try_direct_api_setup() -> None:
    """Try to directly set up Airbyte using API calls if the script fails"""
    print("\nAttempting direct API setup...")

    try:
        # Get API URL and auth status
        api_url = AIRBYTE_API_URL

        # Determine if auth is needed by trying a request
        try:
            no_auth_response = requests.get(f"{api_url}/health", timeout=5)
            needs_auth = no_auth_response.status_code == 401
        except requests.RequestException:
            needs_auth = True

        auth = (BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD) if needs_auth else None

        # Get workspace ID
        print("Getting workspace ID...")
        workspaces_response = requests.post(
            f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
        )

        if workspaces_response.status_code != 200:
            print(f"Failed to get workspaces: {workspaces_response.status_code}")
            return

        workspace_id = workspaces_response.json()["workspaces"][0]["workspaceId"]
        print(f"Workspace ID: {workspace_id}")

        # Just create a minimal test connection for validation
        # Get Postgres source definition
        source_defs_response = requests.post(
            f"{api_url}/source_definitions/list", auth=auth, json={}, timeout=10
        )

        if source_defs_response.status_code != 200:
            print(
                f"Failed to get source definitions: {source_defs_response.status_code}"
            )
            return

        for source_def in source_defs_response.json().get("sourceDefinitions", []):
            if "Postgres" in source_def.get("name", ""):
                postgres_def_id = source_def["sourceDefinitionId"]

                # Create a test Postgres source
                source_response = requests.post(
                    f"{api_url}/sources/create",
                    auth=auth,
                    json={
                        "name": "Test Direct Postgres Source",
                        "sourceDefinitionId": postgres_def_id,
                        "workspaceId": workspace_id,
                        "connectionConfiguration": {
                            "host": "test-postgres",
                            "port": 5432,
                            "database": "test",
                            "schema": "source_schema",
                            "username": "test",
                            "password": "test",
                            "ssl": False,
                        },
                    },
                    timeout=20,
                )

                if source_response.status_code == 200:
                    print("✅ Created test Postgres source")
                else:
                    print(
                        f"⚠️ Failed to create test source: {source_response.status_code}"
                    )

                break
    except Exception as e:
        print(f"⚠️ Direct API setup failed: {str(e)}")


def print_logs_for_debugging(container_name: str, lines: int = 50) -> None:
    """Print logs for a container to help with debugging"""
    print(f"\n=============== {container_name} logs ===============")
    try:
        subprocess.run(
            f"docker logs {container_name} --tail {lines}", shell=True, timeout=10
        )
    except subprocess.SubprocessError:
        print(f"Failed to retrieve logs for {container_name}")
    print(f"=============== End {container_name} logs ===============\n")


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    """Return the path to the test resources directory."""
    return pytestconfig.rootpath / "tests/integration/airbyte"


@pytest.fixture(scope="module")
def set_docker_env_vars() -> Generator[None, None, None]:
    """Set environment variables needed for Docker Compose"""
    env_vars = {
        "VERSION": "0.63.13",
        "DATABASE_USER": "docker",
        "DATABASE_PASSWORD": "docker",
        "DATABASE_DB": "airbyte",
        "DATABASE_URL": "jdbc:postgresql://db:5432/airbyte",
        "CONFIG_ROOT": "/data",
        "WORKSPACE_ROOT": "/tmp/workspace",
        "DATA_DOCKER_MOUNT": "airbyte_data",
        "DB_DOCKER_MOUNT": "airbyte_db",
        "WORKSPACE_DOCKER_MOUNT": "airbyte_workspace",
        "LOCAL_ROOT": "/tmp/airbyte_local",
        "LOCAL_DOCKER_MOUNT": "airbyte_local",
        "HACK_LOCAL_ROOT_PARENT": "/tmp",
        "WEBAPP_URL": "http://localhost:8000/",
        "TEMPORAL_HOST": "airbyte-temporal:7233",
        "AIRBYTE_SERVER_HOST": "airbyte-server:8001",
        "INTERNAL_API_HOST": "http://airbyte-server:8001",
        "CONNECTOR_BUILDER_API_HOST": "airbyte-connector-builder-server:8080",
        "AIRBYTE_API_HOST": "airbyte-server:8001/api/public",
        "TRACKING_STRATEGY": "logging",
        "BASIC_AUTH_USERNAME": BASIC_AUTH_USERNAME,
        "BASIC_AUTH_PASSWORD": BASIC_AUTH_PASSWORD,
        "DATABASE_HOST": "db",
        "DATABASE_PORT": "5432",
        "LOG_LEVEL": "INFO",
        "WORKER_ENVIRONMENT": "control-plane",
        "WORKERS_MICRONAUT_ENVIRONMENTS": "control-plane",
        "SERVER_MICRONAUT_ENVIRONMENTS": "control-plane",
        "CRON_MICRONAUT_ENVIRONMENTS": "control-plane",
        "FEATURE_FLAG_CLIENT": "config",
        "AUTO_DETECT_SCHEMA": "true",
        "STORAGE_TYPE": "local",
        "SECRET_PERSISTENCE": "TESTING_CONFIG_DB_TABLE",
    }
    # Store original values - explicitly handle Optional type
    original_vars: Dict[str, Optional[str]] = {
        key: os.environ.get(key) for key in env_vars
    }

    # Set vars
    for key, env_value in env_vars.items():
        os.environ[key] = env_value

    yield

    # Restore original values - use correct type annotations in the loop
    for key, optional_value in original_vars.items():
        if optional_value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            # Now optional_value is narrowed to str
            str_value: str = optional_value
            os.environ[key] = str_value


@pytest.fixture(scope="module")
def airbyte_service(
    docker_compose_runner: Any,
    pytestconfig: Any,
    test_resources_dir: Path,
    set_docker_env_vars: None,
) -> Generator[Any, None, None]:
    """Start Airbyte and test databases using Docker Compose with better diagnostics"""
    print("\n\nStarting Docker Compose services for Airbyte...\n")

    # Set the default API URL - this may be updated later
    global AIRBYTE_API_URL
    api_url = f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1"
    AIRBYTE_API_URL = api_url

    # If containers are already running, we should stop them first
    try:
        print("Stopping any existing containers...")
        subprocess.run(
            f"docker-compose -f {test_resources_dir}/docker-compose.yml down -v",
            shell=True,
            check=False,
            timeout=60,
        )
        time.sleep(5)  # Give some time for cleanup
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "airbyte"
    ) as docker_services:
        try:
            # Wait for PostgreSQL test database with longer timeout
            print("Waiting for test-postgres...")
            wait_for_port(
                docker_services,
                "test-postgres",
                POSTGRES_PORT,
                timeout=120,
                checker=lambda: is_postgres_ready("test-postgres"),
            )
            print("test-postgres is ready")

            # Wait for MySQL test database with longer timeout
            print("Waiting for test-mysql...")
            wait_for_port(
                docker_services,
                "test-mysql",
                MYSQL_PORT,
                timeout=120,
                checker=lambda: is_mysql_ready("test-mysql"),
            )
            print("test-mysql is ready")

            # Wait for Airbyte server to be available - try both ports
            print("Checking if airbyte-server port is open...")
            try:
                wait_for_port(
                    docker_services,
                    "airbyte-server",
                    8001,
                    timeout=30,
                )
                print("airbyte-server port 8001 is open")
            except Exception:
                print("airbyte-server port 8001 check failed, trying proxy...")
                try:
                    wait_for_port(
                        docker_services,
                        "airbyte-proxy",
                        8001,
                        timeout=30,
                    )
                    print("airbyte-proxy port 8001 is open")
                except Exception:
                    print("WARNING: Both server and proxy port 8001 checks failed")

            # Wait for Airbyte web UI to be available
            try:
                wait_for_port(
                    docker_services,
                    "airbyte-proxy",
                    8000,
                    timeout=30,
                )
                print("airbyte-proxy port 8000 is open")
            except Exception:
                print("WARNING: Proxy port 8000 check failed")

            # Wait for Airbyte API to be ready with more comprehensive checks
            wait_for_airbyte_ready(timeout=600)

            # Set up test connections in Airbyte
            setup_airbyte_connections(test_resources_dir)

            yield docker_services

        except Exception as e:
            # Print logs from key containers to help with debugging
            print(f"Exception during setup: {str(e)}")
            print_logs_for_debugging("airbyte-server")
            print_logs_for_debugging("airbyte-db")
            print_logs_for_debugging("airbyte-proxy")
            print_logs_for_debugging("test-postgres")
            print_logs_for_debugging("test-mysql")
            raise


def update_config_file_with_api_url(config_file: Path) -> Path:
    """Update a YAML config file with the correct API URL if needed"""
    if f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1" == AIRBYTE_API_URL:
        # No changes needed
        return config_file

    try:
        with open(config_file, "r") as f:
            content = f.read()

        # Extract port from API URL
        if ":" in AIRBYTE_API_URL:
            port = AIRBYTE_API_URL.split(":")[2].split("/")[0]
        else:
            port = "8001"  # Default

        # Replace the host_port
        new_content = content.replace(
            "host_port: http://localhost:8001", f"host_port: http://localhost:{port}"
        )

        # Create a modified copy
        import hashlib

        hash_id = hashlib.md5(AIRBYTE_API_URL.encode()).hexdigest()[:8]
        new_file = (
            config_file.parent / f"{config_file.stem}_mod_{hash_id}{config_file.suffix}"
        )

        with open(new_file, "w") as f:
            f.write(new_content)

        print(f"Created modified config with correct API port: {new_file}")
        return new_file
    except Exception as e:
        print(f"Failed to update config file: {str(e)}")
        return config_file


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_airbyte_ingest(
    test_resources_dir: Path,
    airbyte_service: Any,
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    """Test basic Airbyte ingestion"""
    # Get the original config file
    original_config_file = (test_resources_dir / "airbyte_to_file.yml").resolve()

    # Update config with correct API URL if needed
    config_file = update_config_file_with_api_url(original_config_file)

    output_path = tmp_path / "airbyte_mces.json"
    golden_path = test_resources_dir / "airbyte_mces_golden.json"

    print(f"Running ingestion with config: {config_file}")
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output
    assert output_path.exists(), f"Output file {output_path} not created"
    with open(output_path, "r") as f:
        content = f.read()
        assert content, "Output file is empty"

    # Print some summary info about the ingested metadata if not updating golden files
    if not pytestconfig.getoption("--update-golden-files", False):
        try:
            # Try to load as JSON Lines first
            mces: List[Dict[str, Any]] = []
            for line in content.splitlines():
                line = line.strip()
                if line and line not in (
                    "[",
                    "]",
                ):  # Skip empty lines and bare brackets
                    mce = json.loads(line)
                    mces.append(mce)
        except json.JSONDecodeError:
            # If that fails, try loading as a single JSON array
            try:
                mces = json.loads(content)
            except json.JSONDecodeError as e:
                print(f"Failed to parse file content: {content}")
                raise e

        # Count entities by type
        entity_counts: Dict[str, int] = {}
        for mce in mces:
            if "entityType" in mce:
                entity_type = mce["entityType"]
                entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1

        print(f"Ingested entity counts: {entity_counts}")

    # Verify against golden file - this handles --update-golden-files automatically
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=[],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_airbyte_platform_instance_urns(
    test_resources_dir: Path,
    airbyte_service: Any,
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    """Test Airbyte with platform instance URNs"""
    # Get the original config file
    original_config_file = (
        test_resources_dir / "airbyte_platform_instance_to_file.yml"
    ).resolve()

    # Update config with correct API URL if needed
    config_file = update_config_file_with_api_url(original_config_file)

    output_path = tmp_path / "airbyte_platform_instance_mces.json"
    golden_path = test_resources_dir / "airbyte_platform_instance_mces_golden.json"

    print(f"Running ingestion with config: {config_file}")
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output file exists
    assert output_path.exists(), f"Output file {output_path} not created"

    # Verify against golden file - this handles --update-golden-files automatically
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=[],
    )

    # Only check specific assertions if we're not updating golden files
    if not pytestconfig.getoption("--update-golden-files", False):
        with output_path.open() as f:
            content = f.read()
            # Skip if file is empty or just contains brackets
            if content and content.strip() not in ("[]", "[", "]"):
                # Load the MCEs from the output file
                try:
                    # Try to load as JSON Lines first
                    mces: List[Dict[str, Any]] = []
                    for line in content.splitlines():
                        line = line.strip()
                        if line and line not in (
                            "[",
                            "]",
                        ):  # Skip empty lines and brackets
                            mce = json.loads(line)
                            mces.append(mce)
                except json.JSONDecodeError:
                    # If that fails, try loading as a single JSON array
                    try:
                        mces = json.loads(content)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse file content: {content}")
                        raise e

                # Verify the platform instances
                if len(mces) > 0:
                    custom_platform_found = False
                    platform_instances: Set[str] = set()

                    for mce in mces:
                        if "entityType" not in mce:
                            continue

                        # Check dataset URN structure for custom platform instance
                        if mce["entityType"] == "dataset" and "entityUrn" in mce:
                            # Look for the custom platform instance in the URN
                            if "custom-platform" in mce["entityUrn"]:
                                custom_platform_found = True

                            # Extract platform instance for reporting
                            if "platformInstance=" in mce["entityUrn"]:
                                parts = mce["entityUrn"].split("platformInstance=")
                                if len(parts) > 1:
                                    instance = parts[1].split(",")[0].strip(")")
                                    platform_instances.add(instance)

                    print(f"Platform instances found: {platform_instances}")
                    assert custom_platform_found, (
                        "Custom platform instance not found in any dataset URN"
                    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_airbyte_schema_filter(
    test_resources_dir: Path,
    airbyte_service: Any,
    pytestconfig: Any,
    tmp_path: Path,
) -> None:
    """Test Airbyte with schema filtering"""
    # Get the original config file
    original_config_file = (
        test_resources_dir / "airbyte_schema_filter_to_file.yml"
    ).resolve()

    # Update config with correct API URL if needed
    config_file = update_config_file_with_api_url(original_config_file)

    output_path = tmp_path / "airbyte_schema_filter_mces.json"
    golden_path = test_resources_dir / "airbyte_schema_filter_mces_golden.json"

    print(f"Running ingestion with config: {config_file}")
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output file exists
    assert output_path.exists(), f"Output file {output_path} not created"

    # Verify against golden file - this handles --update-golden-files automatically
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
        ignore_paths=[],
    )

    # Only check specific assertions if we're not updating golden files
    if not pytestconfig.getoption("--update-golden-files", False):
        with output_path.open() as f:
            content = f.read()
            # Skip if file is empty or just contains brackets
            if content and content.strip() not in ("[]", "[", "]"):
                # Load the MCEs from the output file
                try:
                    # Try to load as JSON Lines first
                    mces: List[Dict[str, Any]] = []
                    for line in content.splitlines():
                        line = line.strip()
                        if line and line not in (
                            "[",
                            "]",
                        ):  # Skip empty lines and bare brackets
                            mce = json.loads(line)
                            mces.append(mce)
                except json.JSONDecodeError:
                    # If that fails, try loading as a single JSON array
                    try:
                        mces = json.loads(content)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse file content: {content}")
                        raise e

                # Check if filtering worked - we should only have MySQL sources and Postgres destinations
                if len(mces) > 0:
                    datasets_found = False
                    mysql_sources = 0
                    postgres_destinations = 0
                    other_platforms: List[str] = []

                    for mce in mces:
                        if "entityType" not in mce or mce["entityType"] != "dataset":
                            continue

                        datasets_found = True

                        # Extract the dataset platform from the URN
                        if "entityUrn" in mce:
                            urn = mce["entityUrn"]

                            # All source datasets should be from MySQL
                            if "mysql" in urn.lower():
                                mysql_sources += 1

                            # All destination datasets should be from Postgres
                            elif "postgres" in urn.lower():
                                postgres_destinations += 1

                            # Collect any other platforms for debugging
                            else:
                                if "dataPlatform=" in urn:
                                    platform = urn.split("dataPlatform=")[1].split(",")[
                                        0
                                    ]
                                    if platform not in other_platforms:
                                        other_platforms.append(platform)

                    print(f"MySQL source datasets: {mysql_sources}")
                    print(f"Postgres destination datasets: {postgres_destinations}")
                    if other_platforms:
                        print(f"Other platforms found: {other_platforms}")

                    assert datasets_found, "No dataset entities found in output"
                    assert mysql_sources > 0, (
                        "No MySQL source datasets found after filtering"
                    )
                    assert postgres_destinations > 0, (
                        "No Postgres destination datasets found after filtering"
                    )


@pytest.mark.integration
def test_airbyte_connection(
    test_resources_dir: Path,
    airbyte_service: Any,
) -> None:
    """Test the test_connection function of the Airbyte source"""
    from datahub.ingestion.source.airbyte.config import AirbyteSourceConfig
    from datahub.ingestion.source.airbyte.connection import test_connection

    # Extract port from API URL
    if ":" in AIRBYTE_API_URL:
        port = AIRBYTE_API_URL.split(":")[2].split("/")[0]
    else:
        port = "8001"  # Default

    # Create a config for the connection test using the determined port
    config_dict = {
        "deployment_type": "oss",
        "host_port": f"http://localhost:{port}",
        "username": BASIC_AUTH_USERNAME,
        "password": BASIC_AUTH_PASSWORD,
        "verify_ssl": False,
    }

    print(f"Testing connection with: {config_dict}")
    config = AirbyteSourceConfig.parse_obj(config_dict)

    # Test the connection
    error = test_connection(config)

    # Verify there are no errors
    assert error is None, f"Connection test failed with error: {error}"
