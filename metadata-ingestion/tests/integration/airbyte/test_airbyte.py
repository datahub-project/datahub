import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import pytest
import requests
from freezegun import freeze_time

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

FROZEN_TIME = "2023-10-15 07:00:00"
# We'll check and dynamically determine the working API port
AIRBYTE_API_HOST = "localhost"
AIRBYTE_WEB_PORT = 8000
AIRBYTE_API_PORT = 8000  # abctl-based setup maps server to port 8000
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

    # Try the main API port (both web and API are on 8000 in abctl-based setup)
    for port in [AIRBYTE_API_PORT]:
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
                    print(f"Found working API URL: {url} (auth: {auth_type})")
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
    """Run diagnostics for abctl Kubernetes-based Airbyte deployment"""
    print("\nDIAGNOSING AIRBYTE ISSUE")

    # For abctl, check Kubernetes status instead of Docker containers
    print("Checking abctl status...")
    try:
        # Check abctl status
        result = subprocess.run(
            ["abctl", "local", "status"], capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print("abctl status:")
            print(result.stdout)
        else:
            print(f"abctl status failed: {result.stderr}")
    except Exception as e:
        print(f"Error checking abctl status: {e}")

    # Check for port conflicts
    print(f"\nChecking port {AIRBYTE_API_PORT}...")
    try:
        result = subprocess.run(
            ["lsof", "-i", f":{AIRBYTE_API_PORT}"], capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            print(f"Port {AIRBYTE_API_PORT} is in use:")
            print(result.stdout)
        else:
            print(f"No conflicts detected on port {AIRBYTE_API_PORT}")
    except Exception as e:
        print(f"Error checking port {AIRBYTE_API_PORT}: {str(e)}")

    # Try to check Kubernetes pods if kubeconfig is available
    print("\nChecking Kubernetes pods...")
    try:
        kubeconfig = "/Users/jonny/.airbyte/abctl/abctl.kubeconfig"
        result = subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                kubeconfig,
                "get",
                "pods",
                "-n",
                "airbyte-abctl",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            print("Kubernetes pods status:")
            print(result.stdout)
        else:
            print("Could not get Kubernetes pods status (cluster may be cleaned up)")
            print(result.stderr)
    except Exception as e:
        print(f"Error checking Kubernetes pods: {e}")

    print("\n END OF DIAGNOSTICS\n")


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
        print("WARNING: Could not find working API URL, using default with auth")
        return (f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1", True)


def wait_for_airbyte_ready(timeout: int = 600) -> bool:
    """Wait for Airbyte API to be ready - optimized for abctl Kubernetes deployment"""
    print(f"Waiting for Airbyte API to be ready (timeout: {timeout}s)...")

    # For abctl, we primarily rely on the health endpoint since containers run in Kubernetes
    end_time = time.time() + timeout
    check_interval = 10  # seconds
    attempt = 0

    while time.time() < end_time:
        attempt += 1
        print(f"\nAttempt {attempt}: Testing Airbyte API connectivity...")

        # Try to determine a working API URL
        global AIRBYTE_API_URL
        api_url, needs_auth = get_api_url_for_test()
        AIRBYTE_API_URL = api_url
        print(f"Testing API URL: {api_url} (Auth: {needs_auth})")

        try:
            # First check the health endpoint (should work without auth)
            health_response = requests.get(f"{api_url}/health", timeout=10)
            if health_response.status_code == 200:
                print("Health endpoint is responding")

                # If health is good, try a simple authenticated request
                if needs_auth:
                    # Try to get workspaces list with auth
                    auth = (BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD)
                    workspaces_response = requests.post(
                        f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
                    )

                    if workspaces_response.status_code == 200:
                        print(
                            "Successfully connected to Airbyte API and retrieved workspaces!"
                        )
                        return True
                    elif workspaces_response.status_code == 401:
                        print(
                            "API is running but authentication failed - this may be expected for initial setup"
                        )
                        # For integration tests, API being responsive is often sufficient
                        return True
                    else:
                        print(
                            f"API request failed: {workspaces_response.status_code} - {workspaces_response.text[:100]}"
                        )
                else:
                    # No auth required, health endpoint working is sufficient
                    print("API is responding without authentication - ready!")
                    return True
            else:
                print(f"Health endpoint returned: {health_response.status_code}")

        except requests.RequestException as e:
            print(f"API connection error: {type(e).__name__}: {str(e)}")

        # Wait before next check
        if attempt < 5:  # Only show countdown for first few attempts
            print(f"Waiting {check_interval} seconds before next check...")
        else:
            print("Still waiting for API to be ready...")
        time.sleep(check_interval)

    # If we got here, we timed out
    print("ERROR: Timeout waiting for Airbyte API!")
    print("Note: abctl uses Kubernetes pods, not Docker containers")
    raise TimeoutError(f"Airbyte API did not become ready within {timeout} seconds")


def setup_airbyte_using_script(test_resources_dir: Path) -> None:
    """Set up Airbyte using the existing setup script"""
    print("Setting up Airbyte using setup_airbyte.sh script...")

    try:
        setup_script = test_resources_dir / "setup" / "setup_airbyte.sh"
        if not setup_script.exists():
            print(f"WARNING: Setup script not found at {setup_script}")
            return

        # Make sure the script is executable
        setup_script.chmod(0o755)

        # Run the setup script
        print(f"Running setup script: {setup_script}")
        result = subprocess.run(
            [str(setup_script)],
            cwd=test_resources_dir,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes timeout
        )

        if result.returncode == 0:
            print("SUCCESS: Airbyte setup script completed successfully")
            if result.stdout:
                print("Script output:")
                print(result.stdout[-1000:])  # Show last 1000 chars to avoid spam
        else:
            print(f"WARNING: Setup script failed with return code {result.returncode}")
            if result.stderr:
                print("Script errors:")
                print(result.stderr[-1000:])
            if result.stdout:
                print("Script output:")
                print(result.stdout[-1000:])

    except subprocess.TimeoutExpired:
        print("WARNING: Setup script timed out after 5 minutes")
    except Exception as e:
        print(f"WARNING: Failed to run setup script: {str(e)}")


def setup_airbyte_connections(test_resources_dir: Path) -> None:
    """Set up Airbyte sources, destinations, and connections for testing"""
    print("Setting up Airbyte sources, destinations, and connections...")

    # First verify the API is working
    workspace_id = try_direct_api_setup()

    if workspace_id:
        # Create the actual sources and destinations
        create_airbyte_test_setup(workspace_id)


def try_direct_api_setup() -> str:
    """Verify Airbyte API is working and return workspace ID"""
    print("\nVerifying Airbyte API is working...")

    try:
        # Get API URL and auth status
        api_url = AIRBYTE_API_URL

        # abctl enables auth by default, so we need to check and provide credentials
        try:
            no_auth_response = requests.get(f"{api_url}/health", timeout=5)
            needs_auth = no_auth_response.status_code == 401
        except requests.RequestException:
            needs_auth = True  # Default to auth required for abctl setup

        # Use retrieved password if available, otherwise fall back to default
        password = os.environ.get("AIRBYTE_PASSWORD", BASIC_AUTH_PASSWORD)
        auth = (BASIC_AUTH_USERNAME, password) if needs_auth else None

        # Get workspace ID
        print("Getting workspace ID...")
        workspaces_response = requests.post(
            f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
        )

        if workspaces_response.status_code != 200:
            print(f"Failed to get workspaces: {workspaces_response.status_code}")
            return None

        workspace_id = workspaces_response.json()["workspaces"][0]["workspaceId"]
        print(f"SUCCESS: Workspace ID: {workspace_id}")

        # Verify we can get source definitions (this validates the API is fully functional)
        source_defs_response = requests.post(
            f"{api_url}/source_definitions/list", auth=auth, json={}, timeout=10
        )

        if source_defs_response.status_code == 200:
            source_defs = source_defs_response.json().get("sourceDefinitions", [])
            print(
                f"SUCCESS: API is working - found {len(source_defs)} source definitions"
            )
            return workspace_id
        else:
            print(
                f"WARNING: Failed to get source definitions: {source_defs_response.status_code}"
            )
            return None

    except Exception as e:
        print(f"WARNING: API setup verification failed: {str(e)}")
        return None


def create_airbyte_test_setup(workspace_id: str) -> None:
    """Create PostgreSQL and MySQL sources, PostgreSQL destination, and connections"""
    print(f"\nCreating Airbyte test setup for workspace {workspace_id}...")

    try:
        # Get API URL and auth
        api_url = AIRBYTE_API_URL
        password = os.environ.get("AIRBYTE_PASSWORD", BASIC_AUTH_PASSWORD)
        auth = (BASIC_AUTH_USERNAME, password)

        # Get source definitions
        source_defs_response = requests.post(
            f"{api_url}/source_definitions/list", auth=auth, json={}, timeout=10
        )

        if source_defs_response.status_code != 200:
            print(
                f"Failed to get source definitions: {source_defs_response.status_code}"
            )
            return

        source_defs = source_defs_response.json().get("sourceDefinitions", [])

        # Find PostgreSQL and MySQL source definition IDs
        postgres_source_def_id = None
        mysql_source_def_id = None

        for source_def in source_defs:
            name = source_def.get("name", "").lower()
            if "postgres" in name and not postgres_source_def_id:
                postgres_source_def_id = source_def["sourceDefinitionId"]
                print(
                    f"Found PostgreSQL source definition: {source_def['name']} ({postgres_source_def_id})"
                )
            elif "mysql" in name and not mysql_source_def_id:
                mysql_source_def_id = source_def["sourceDefinitionId"]
                print(
                    f"Found MySQL source definition: {source_def['name']} ({mysql_source_def_id})"
                )

        if not postgres_source_def_id:
            print("WARNING: Could not find PostgreSQL source definition")
            return

        if not mysql_source_def_id:
            print("WARNING: Could not find MySQL source definition")
            return

        # Create PostgreSQL source
        print("Creating PostgreSQL source...")
        postgres_source_config = {
            "name": "Test Postgres Source",
            "sourceDefinitionId": postgres_source_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": POSTGRES_PORT,  # 5433
                "database": "test",
                "username": "test",
                "password": "test",
                "ssl": False,
                "ssl_mode": {"mode": "disable"},
            },
        }

        postgres_source_response = requests.post(
            f"{api_url}/sources/create",
            auth=auth,
            json=postgres_source_config,
            timeout=10,
        )

        if postgres_source_response.status_code in [200, 201]:
            postgres_source_id = postgres_source_response.json()["sourceId"]
            print(f"SUCCESS: Created PostgreSQL source with ID: {postgres_source_id}")
        else:
            print(
                f"Failed to create PostgreSQL source: {postgres_source_response.status_code}"
            )
            print(f"Response: {postgres_source_response.text}")
            return

        # Create MySQL source
        print("Creating MySQL source...")
        mysql_source_config = {
            "name": "Test MySQL Source",
            "sourceDefinitionId": mysql_source_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": MYSQL_PORT,  # 30306
                "database": "test",
                "username": "test",
                "password": "test",
                "ssl": False,
            },
        }

        mysql_source_response = requests.post(
            f"{api_url}/sources/create", auth=auth, json=mysql_source_config, timeout=10
        )

        if mysql_source_response.status_code in [200, 201]:
            mysql_source_id = mysql_source_response.json()["sourceId"]
            print(f"SUCCESS: Created MySQL source with ID: {mysql_source_id}")
        else:
            print(f"Failed to create MySQL source: {mysql_source_response.status_code}")
            print(f"Response: {mysql_source_response.text}")
            return

        # Get destination definitions
        dest_defs_response = requests.post(
            f"{api_url}/destination_definitions/list", auth=auth, json={}, timeout=10
        )

        if dest_defs_response.status_code != 200:
            print(
                f"Failed to get destination definitions: {dest_defs_response.status_code}"
            )
            return

        dest_defs = dest_defs_response.json().get("destinationDefinitions", [])

        # Find PostgreSQL destination definition ID
        postgres_dest_def_id = None
        for dest_def in dest_defs:
            name = dest_def.get("name", "").lower()
            if "postgres" in name and not postgres_dest_def_id:
                postgres_dest_def_id = dest_def["destinationDefinitionId"]
                print(
                    f"Found PostgreSQL destination definition: {dest_def['name']} ({postgres_dest_def_id})"
                )
                break

        if not postgres_dest_def_id:
            print("WARNING: Could not find PostgreSQL destination definition")
            return

        # Create PostgreSQL destination
        print("Creating PostgreSQL destination...")
        postgres_dest_config = {
            "name": "Test Postgres Destination",
            "destinationDefinitionId": postgres_dest_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": POSTGRES_PORT,  # 5433
                "database": "test",
                "username": "test",
                "password": "test",
                "ssl": False,
                "ssl_mode": {"mode": "disable"},
                "schema": "public",
            },
        }

        postgres_dest_response = requests.post(
            f"{api_url}/destinations/create",
            auth=auth,
            json=postgres_dest_config,
            timeout=10,
        )

        if postgres_dest_response.status_code in [200, 201]:
            postgres_dest_id = postgres_dest_response.json()["destinationId"]
            print(
                f"SUCCESS: Created PostgreSQL destination with ID: {postgres_dest_id}"
            )
        else:
            print(
                f"Failed to create PostgreSQL destination: {postgres_dest_response.status_code}"
            )
            print(f"Response: {postgres_dest_response.text}")
            return

        print("SUCCESS: All Airbyte sources and destinations created successfully!")
        print(f"PostgreSQL Source ID: {postgres_source_id}")
        print(f"MySQL Source ID: {mysql_source_id}")
        print(f"PostgreSQL Destination ID: {postgres_dest_id}")

    except Exception as e:
        print(f"ERROR: Failed to create Airbyte test setup: {str(e)}")
        import traceback

        traceback.print_exc()


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


def init_test_data():
    """Initialize test data in PostgreSQL and MySQL containers using SQL files"""
    test_resources_dir = Path(__file__).parent

    try:
        # Initialize PostgreSQL test data from SQL file
        postgres_init_file = test_resources_dir / "setup" / "init-test-db.sql"

        if postgres_init_file.exists():
            print(" Initializing PostgreSQL with init-test-db.sql...")
            with open(postgres_init_file, "r") as f:
                postgres_sql = f.read()

            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "test-postgres",
                    "psql",
                    "-U",
                    "test",
                    "-d",
                    "test",
                    "-c",
                    postgres_sql,
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                print(f"PostgreSQL init warning: {result.stderr}")
            else:
                print("SUCCESS: PostgreSQL test data initialized from SQL file")
        else:
            print("WARNING: PostgreSQL init file not found, skipping")

    except Exception as e:
        print(f"PostgreSQL init error: {e}")

    try:
        # Initialize MySQL test data from SQL file
        mysql_init_file = test_resources_dir / "setup" / "init-test-mysql.sql"

        if mysql_init_file.exists():
            print(" Initializing MySQL with init-test-mysql.sql...")
            with open(mysql_init_file, "r") as f:
                mysql_sql = f.read()

            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "test-mysql",
                    "mysql",
                    "-u",
                    "root",
                    "-prootpwd123",
                    "-e",
                    mysql_sql,
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                print(f"MySQL init warning: {result.stderr}")
            else:
                print("SUCCESS: MySQL test data initialized from SQL file")
        else:
            print("WARNING: MySQL init file not found, skipping")

    except Exception as e:
        print(f"MySQL init error: {e}")


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig: Any) -> Path:
    """Return the path to the test resources directory."""
    return pytestconfig.rootpath / "tests/integration/airbyte"


@pytest.fixture(scope="module")
def test_databases(test_resources_dir: Path, docker_compose_runner):
    """Start PostgreSQL and MySQL test databases"""
    print("\nStarting test databases...")

    # Use docker-compose for test databases only
    compose_file = test_resources_dir / "docker-compose.yml"

    with docker_compose_runner(compose_file, "airbyte-test-dbs") as docker_services:
        # Wait for databases to be ready
        print("Waiting for test databases to be ready...")

        # Wait for PostgreSQL
        max_attempts = 30
        for attempt in range(max_attempts):
            if is_postgres_ready("test-postgres"):
                print("SUCCESS: PostgreSQL test database is ready!")
                break
            print(f"PostgreSQL not ready yet (attempt {attempt + 1}/{max_attempts})")
            time.sleep(2)
        else:
            raise RuntimeError("PostgreSQL test database failed to start")

        # Wait for MySQL
        for attempt in range(max_attempts):
            if is_mysql_ready("test-mysql"):
                print("SUCCESS: MySQL test database is ready!")
                break
            print(f"MySQL not ready yet (attempt {attempt + 1}/{max_attempts})")
            time.sleep(2)
        else:
            raise RuntimeError("MySQL test database failed to start")

        # Initialize test data
        print("Initializing test data...")
        init_test_data()

        yield docker_services

        print("Test databases cleaned up")


@pytest.fixture(scope="module")
def set_docker_env_vars() -> Generator[None, None, None]:
    """Set environment variables needed for abctl setup"""
    # abctl handles most configuration automatically,
    # so we only need minimal env vars for compatibility
    env_vars = {
        "BASIC_AUTH_USERNAME": BASIC_AUTH_USERNAME,
        "BASIC_AUTH_PASSWORD": BASIC_AUTH_PASSWORD,
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


def install_abctl(test_resources_dir: Path) -> Path:
    """Install abctl if not already available, optimized for CI environments"""
    print("Installing abctl...")

    # Check if abctl is already installed and working
    try:
        result = subprocess.run(
            ["abctl", "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print(f"abctl already installed: {result.stdout.strip()}")
            return Path("abctl")  # Return the system abctl
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # For CI environments, download abctl directly
    print("Downloading abctl for CI environment...")

    # Determine OS and architecture
    import platform

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        os_name = "darwin"
        arch = "arm64" if machine == "arm64" else "amd64"
    elif system == "linux":
        os_name = "linux"
        arch = "arm64" if machine in ["aarch64", "arm64"] else "amd64"
    else:
        raise RuntimeError(f"Unsupported OS: {system}")

    # Use a known working version for stability in CI
    version = "v0.15.0"
    download_url = f"https://github.com/airbytehq/abctl/releases/download/{version}/abctl-{os_name}-{arch}"

    abctl_path = test_resources_dir / "abctl"

    print(f"Downloading {download_url}...")
    try:
        import urllib.request

        urllib.request.urlretrieve(download_url, abctl_path)
        abctl_path.chmod(0o755)

        # Verify the download worked
        result = subprocess.run(
            [str(abctl_path), "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            raise RuntimeError(f"Downloaded abctl is not working: {result.stderr}")

        print(f"abctl {version} installed successfully for {os_name}-{arch}")
        return abctl_path

    except Exception as e:
        raise RuntimeError(f"Failed to download abctl: {e}") from e


def _start_airbyte_with_abctl(abctl_path: Path, test_resources_dir: Path) -> None:
    """Start Airbyte using abctl with optimized CI configuration."""
    print("Starting Airbyte with abctl (headless mode for CI)...")

    install_cmd = [
        str(abctl_path),
        "local",
        "install",
        "--port",
        str(AIRBYTE_API_PORT),
        "--no-browser",
        "--insecure-cookies",
    ]

    print(f"Running: {' '.join(install_cmd)}")
    result = subprocess.run(
        install_cmd,
        cwd=test_resources_dir,
        capture_output=True,
        text=True,
        timeout=900,  # 15 minutes timeout for installation
    )

    if result.returncode != 0:
        print(f"abctl install failed: {result.stderr}")
        print(f"stdout: {result.stdout}")
        raise RuntimeError(f"Failed to start Airbyte with abctl: {result.stderr}")

    print("SUCCESS: abctl install completed")
    print(result.stdout)


def _get_airbyte_credentials(abctl_path: Path, test_resources_dir: Path) -> None:
    """Retrieve and store Airbyte credentials from abctl."""
    print("Getting Airbyte credentials...")
    try:
        creds_result = subprocess.run(
            [str(abctl_path), "local", "credentials"],
            cwd=test_resources_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if creds_result.returncode == 0:
            lines = creds_result.stdout.split("\n")
            password = None
            client_id = None
            client_secret = None

            import re

            for line in lines:
                if "Password:" in line:
                    password = line.split("Password:")[1].strip()
                    password = re.sub(r"\x1b\[[0-9;]*m", "", password)
                elif "Client-Id:" in line:
                    client_id = line.split("Client-Id:")[1].strip()
                    client_id = re.sub(r"\x1b\[[0-9;]*m", "", client_id)
                elif "Client-Secret:" in line:
                    client_secret = line.split("Client-Secret:")[1].strip()
                    client_secret = re.sub(r"\x1b\[[0-9;]*m", "", client_secret)

            if password:
                print("SUCCESS: Retrieved Airbyte credentials")
                os.environ["AIRBYTE_PASSWORD"] = password
                if client_id:
                    os.environ["AIRBYTE_CLIENT_ID"] = client_id
                if client_secret:
                    os.environ["AIRBYTE_CLIENT_SECRET"] = client_secret
            else:
                print("WARNING: Could not parse password from credentials output")
        else:
            print(f"Failed to get credentials: {creds_result.stderr}")
    except Exception as e:
        print(f"Error getting credentials: {e}")


def _complete_airbyte_onboarding() -> None:
    """Complete Airbyte onboarding programmatically."""
    print("Attempting to complete Airbyte onboarding programmatically...")
    try:
        import requests

        api_endpoints = [
            f"http://localhost:{AIRBYTE_API_PORT}/api/v1",
            f"http://localhost:{AIRBYTE_API_PORT}/api/public/v1",
        ]

        for api_endpoint in api_endpoints:
            try:
                print(
                    f"Trying onboarding setup at {api_endpoint}/instance_configuration/setup"
                )

                password = os.environ.get("AIRBYTE_PASSWORD", "password")
                auth = ("test@datahub.io", password)

                setup_data = {
                    "email": "test@datahub.io",
                    "anonymousDataCollection": False,
                    "news": False,
                    "securityUpdates": False,
                }

                response = requests.post(
                    f"{api_endpoint}/instance_configuration/setup",
                    json=setup_data,
                    auth=auth,
                    timeout=10,
                )

                if response.status_code in [200, 201]:
                    print("SUCCESS: Onboarding setup completed programmatically")
                    break
                else:
                    print(
                        f"Onboarding setup returned {response.status_code}: {response.text[:200]}..."
                    )

            except requests.RequestException as e:
                print(f"Onboarding setup failed for {api_endpoint}: {e}")
                continue
        else:
            print(
                "Could not complete onboarding setup programmatically, continuing anyway..."
            )

    except Exception as e:
        print(f"Exception during onboarding setup: {e}")


def _cleanup_airbyte(abctl_path: Path, test_resources_dir: Path) -> None:
    """Clean up Airbyte and Kubernetes environment."""
    print("Cleaning up Airbyte environment...")
    try:
        cleanup_result = subprocess.run(
            [str(abctl_path), "local", "uninstall", "--persisted"],
            cwd=test_resources_dir,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if cleanup_result.returncode == 0:
            print("Airbyte and Kubernetes environment cleaned up successfully")
        else:
            print(f"Cleanup failed: {cleanup_result.stderr}")
            # Try fallback cleanup
            fallback_result = subprocess.run(
                [str(abctl_path), "local", "uninstall"],
                cwd=test_resources_dir,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if fallback_result.returncode == 0:
                print("Basic Airbyte uninstall completed")
            else:
                print(
                    f"WARNING: Fallback uninstall also failed: {fallback_result.stderr}"
                )

    except subprocess.TimeoutExpired:
        print("WARNING: Airbyte cleanup timed out")
    except Exception as e:
        print(f"WARNING: Error during Airbyte cleanup: {e}")


@pytest.fixture(scope="module")
def airbyte_service(  # noqa: C901
    pytestconfig: Any,
    test_resources_dir: Path,
    set_docker_env_vars: None,
    test_databases: Any,  # Ensure test databases are started first
) -> Generator[Any, None, None]:
    """Start Airbyte using abctl with headless, low-resource setup for CI"""
    print("\n\nStarting Airbyte using abctl (headless, low-resource setup)...\n")

    # Set the default API URL - abctl uses port 8000 by default
    global AIRBYTE_API_URL
    api_url = f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1"
    AIRBYTE_API_URL = api_url

    # Install abctl
    abctl_path = install_abctl(test_resources_dir)

    # Stop any existing Airbyte instances
    try:
        print("Stopping any existing Airbyte instances...")
        subprocess.run(
            [str(abctl_path), "local", "uninstall", "--persisted"],
            cwd=test_resources_dir,
            capture_output=True,
            timeout=60,
        )
        time.sleep(5)  # Give some time for cleanup
    except Exception as e:
        print(f"Cleanup warning (expected if no previous installation): {str(e)}")

    try:
        # Start Airbyte with headless configuration optimized for CI
        print("Starting Airbyte with abctl (headless mode for CI)...")

        # Use abctl local install with working configuration
        install_cmd = [
            str(abctl_path),
            "local",
            "install",
            "--port",
            str(AIRBYTE_API_PORT),  # Set the port
            "--no-browser",  # Don't launch browser in CI
            "--insecure-cookies",  # Help with authentication issues
            # Note: Removed --low-resource-mode as it causes bootloader to fail
            # GitHub Actions has sufficient resources (2 cores, 7GB RAM)
        ]

        print(f"Running: {' '.join(install_cmd)}")
        result = subprocess.run(
            install_cmd,
            cwd=test_resources_dir,
            capture_output=True,
            text=True,
            timeout=900,  # 15 minutes timeout for installation - bootloader needs time
        )

        if result.returncode != 0:
            print(f"abctl install failed: {result.stderr}")
            print(f"stdout: {result.stdout}")
            raise RuntimeError(f"Failed to start Airbyte with abctl: {result.stderr}")

        print("SUCCESS: abctl install completed")
        print(result.stdout)

        # Get the credentials for API access first (before API readiness check)
        print("Getting Airbyte credentials...")
        try:
            creds_result = subprocess.run(
                [str(abctl_path), "local", "credentials"],
                cwd=test_resources_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if creds_result.returncode == 0:
                # Parse credentials from output
                lines = creds_result.stdout.split("\n")
                # Parse all credentials from abctl output
                password = None
                client_id = None
                client_secret = None

                for line in lines:
                    if "Password:" in line:
                        password = line.split("Password:")[1].strip()
                        # Remove ANSI escape codes that may be in the password
                        import re

                        password = re.sub(r"\x1b\[[0-9;]*m", "", password)
                    elif "Client-Id:" in line:
                        client_id = line.split("Client-Id:")[1].strip()
                        import re

                        client_id = re.sub(r"\x1b\[[0-9;]*m", "", client_id)
                    elif "Client-Secret:" in line:
                        client_secret = line.split("Client-Secret:")[1].strip()
                        import re

                        client_secret = re.sub(r"\x1b\[[0-9;]*m", "", client_secret)

                if password:
                    print("SUCCESS: Retrieved Airbyte credentials")
                    # Store credentials for later use
                    os.environ["AIRBYTE_PASSWORD"] = password
                    if client_id:
                        os.environ["AIRBYTE_CLIENT_ID"] = client_id
                    if client_secret:
                        os.environ["AIRBYTE_CLIENT_SECRET"] = client_secret
                else:
                    print("WARNING: Could not parse password from credentials output")
        except Exception as e:
            print(f"WARNING: Could not retrieve credentials: {e}")
            print("Will attempt to use default credentials")

        # Complete onboarding programmatically (similar to setup_airbyte.sh)
        print("Attempting to complete Airbyte onboarding programmatically...")
        try:
            import requests

            # Try different API endpoints that might work
            api_endpoints = [
                f"http://localhost:{AIRBYTE_API_PORT}/api/v1",
                f"http://localhost:{AIRBYTE_API_PORT}/api/public/v1",
            ]

            for api_endpoint in api_endpoints:
                try:
                    print(
                        f"Trying onboarding setup at {api_endpoint}/instance_configuration/setup"
                    )

                    # Use basic auth with retrieved credentials if available
                    password = os.environ.get("AIRBYTE_PASSWORD", "password")
                    auth = ("test@datahub.io", password)

                    setup_data = {
                        "email": "test@datahub.io",
                        "anonymousDataCollection": False,
                        "news": False,
                        "securityUpdates": False,
                    }

                    response = requests.post(
                        f"{api_endpoint}/instance_configuration/setup",
                        json=setup_data,
                        auth=auth,
                        timeout=10,
                    )

                    if response.status_code in [200, 201]:
                        print("SUCCESS: Onboarding setup completed programmatically")
                        break
                    else:
                        print(
                            f"Onboarding setup returned {response.status_code}: {response.text[:200]}..."
                        )

                except requests.RequestException as e:
                    print(f"Onboarding setup failed for {api_endpoint}: {e}")
                    continue
            else:
                print(
                    "Could not complete onboarding setup programmatically, continuing anyway..."
                )

        except Exception as e:
            print(f"Exception during onboarding setup: {e}")
            print("Continuing with normal API readiness check...")

        # Wait for Airbyte API to be ready (after getting credentials and setup attempt)
        wait_for_airbyte_ready(timeout=300)  # 5 minutes should be enough after install

        # Set up test connections in Airbyte using the setup script
        setup_airbyte_using_script(test_resources_dir)

        yield None  # We don't need to return docker_services for abctl

    except Exception as e:
        print(f"Exception during abctl setup: {str(e)}")
        # Try to get abctl status for debugging
        try:
            status_result = subprocess.run(
                [str(abctl_path), "local", "status"],
                cwd=test_resources_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            print(f"abctl status: {status_result.stdout}")
            if status_result.stderr:
                print(f"abctl status errors: {status_result.stderr}")
        except Exception:
            print("Could not get abctl status")
        raise

    finally:
        # Complete cleanup of Airbyte and Kubernetes environment
        print("Cleaning up Airbyte environment...")
        try:
            # First try to uninstall with persisted data removal
            cleanup_result = subprocess.run(
                [str(abctl_path), "local", "uninstall", "--persisted"],
                cwd=test_resources_dir,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if cleanup_result.returncode == 0:
                print("Airbyte and Kubernetes environment cleaned up successfully")
                if cleanup_result.stdout:
                    print(cleanup_result.stdout)
            else:
                print(f"WARNING: Cleanup had issues: {cleanup_result.stderr}")
                # Try basic uninstall as fallback
                fallback_result = subprocess.run(
                    [str(abctl_path), "local", "uninstall"],
                    cwd=test_resources_dir,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if fallback_result.returncode == 0:
                    print("Basic Airbyte uninstall completed")
                else:
                    print(
                        f"WARNING: Fallback uninstall also failed: {fallback_result.stderr}"
                    )

        except subprocess.TimeoutExpired:
            print("WARNING: Airbyte cleanup timed out")
        except Exception as e:
            print(f"WARNING: Error during Airbyte cleanup: {e}")

        # Clean up downloaded abctl binary if we created one
        try:
            if abctl_path != Path("abctl") and abctl_path.exists():
                abctl_path.unlink()
                print("Removed downloaded abctl binary")
        except Exception as e:
            print(f"WARNING: Could not remove abctl binary: {e}")


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
