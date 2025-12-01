"""
Airbyte integration test setup utilities.

This module contains helper functions for setting up and managing Airbyte
during integration tests. It handles:
- Container readiness checks
- API connectivity and diagnostics
- Database ID updates for consistent golden files
- Source/destination/connection creation
- abctl installation and management
"""

import json
import os
import re
import shutil
import subprocess
import tarfile
import time
import traceback
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Constants
AIRBYTE_API_HOST = "localhost"
AIRBYTE_WEB_PORT = 8000
AIRBYTE_API_PORT = 8000
BASIC_AUTH_USERNAME = "test@datahub.io"
BASIC_AUTH_PASSWORD = "password"
POSTGRES_PORT = 5433
MYSQL_PORT = 30306

# Static IDs for consistent golden files
STATIC_WORKSPACE_ID = "12345678-1234-1234-1234-123456789012"
STATIC_POSTGRES_SOURCE_ID = "11111111-1111-1111-1111-111111111111"
STATIC_MYSQL_SOURCE_ID = "22222222-2222-2222-2222-222222222222"
STATIC_POSTGRES_DEST_ID = "44444444-4444-4444-4444-444444444444"
STATIC_PG_TO_PG_CONNECTION_ID = "55555555-5555-5555-5555-555555555555"
STATIC_MYSQL_TO_PG_CONNECTION_ID = "66666666-6666-6666-6666-666666666666"

# Global API URL (will be updated dynamically)
AIRBYTE_API_URL: str = f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1"


def set_airbyte_api_url(url: str) -> None:
    """Update the global API URL."""
    global AIRBYTE_API_URL
    AIRBYTE_API_URL = url


def get_airbyte_api_url() -> str:
    """Get the current API URL."""
    return AIRBYTE_API_URL


# =============================================================================
# Container Readiness Checks
# =============================================================================


def is_postgres_ready(container_name: str) -> bool:
    """Check if PostgreSQL is responsive on a container."""
    try:
        cmd = f"docker exec {container_name} pg_isready -U test"
        ret = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
        return ret.returncode == 0
    except subprocess.SubprocessError:
        return False


def is_mysql_ready(container_name: str) -> bool:
    """Check if MySQL is responsive on a container."""
    try:
        cmd = f"docker exec {container_name} mysqladmin ping -h localhost -u root -prootpwd123"
        ret = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
        return ret.returncode == 0
    except subprocess.SubprocessError:
        return False


def check_container_logs_for_errors(
    container_name: str, error_patterns: Optional[List[str]] = None
) -> bool:
    """Check if container logs contain specific error patterns."""
    if error_patterns is None:
        error_patterns = ["error", "exception", "failed"]

    try:
        logs = subprocess.check_output(
            f"docker logs {container_name} 2>&1 | grep -i -E '({'|'.join(error_patterns)})' | tail -20",
            shell=True,
            text=True,
        )
        return bool(logs.strip())
    except subprocess.CalledProcessError:
        return False
    except Exception:
        return False


def check_container_health(container_name: str) -> Optional[str]:
    """Check container health status if available."""
    try:
        health = subprocess.check_output(
            f"docker inspect --format='{{{{.State.Health.Status}}}}' {container_name}",
            shell=True,
            text=True,
        ).strip()
        return health if health else None
    except Exception:
        return None


# =============================================================================
# API Connectivity and Diagnostics
# =============================================================================


def find_working_api_url() -> Optional[Dict[str, Any]]:
    """Determine which port and path combination works for the Airbyte API."""
    api_paths = [
        "/api/v1/health",
        "/api/public/v1/health",
        "/api/public/health",
        "/health",
    ]

    for port in [AIRBYTE_API_PORT]:
        for path in api_paths:
            url = f"http://{AIRBYTE_API_HOST}:{port}{path}"
            try:
                response = requests.get(url, timeout=5)
                auth_type = "none"

                if response.status_code == 401:
                    response = requests.get(
                        url, auth=(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD), timeout=5
                    )
                    auth_type = "basic"

                if response.status_code == 200 and (
                    "available" in response.text.lower()
                    or "ok" in response.text.lower()
                ):
                    return {
                        "url": url,
                        "port": port,
                        "path": path,
                        "auth_required": auth_type == "basic",
                    }
            except requests.RequestException:
                pass

    return None


def diagnose_airbyte_proxy_issue() -> None:
    """Run diagnostics for abctl Kubernetes-based Airbyte deployment."""
    print("Running Airbyte diagnostics...")

    try:
        result = subprocess.run(
            ["abctl", "local", "status"], capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(result.stdout)
    except Exception:
        pass

    try:
        kubeconfig = str(Path.home() / ".airbyte" / "abctl" / "abctl.kubeconfig")
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
            print(result.stdout)
    except Exception:
        pass


def get_api_url_for_test() -> Tuple[str, bool]:
    """Determine the appropriate API URL for testing, with fallbacks."""
    api_config = find_working_api_url()

    if api_config:
        if api_config["auth_required"]:
            return (f"http://{AIRBYTE_API_HOST}:{api_config['port']}/api/v1", True)
        else:
            return (f"http://{AIRBYTE_API_HOST}:{api_config['port']}/api/v1", False)
    else:
        diagnose_airbyte_proxy_issue()
        return (f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1", True)


def check_airbyte_pods_ready() -> bool:
    """Check if Airbyte Kubernetes pods are all running."""
    try:
        kubeconfig = Path.home() / ".airbyte" / "abctl" / "abctl.kubeconfig"
        if not kubeconfig.exists():
            return True

        result = subprocess.run(
            [
                "kubectl",
                f"--kubeconfig={kubeconfig}",
                "get",
                "pods",
                "-n",
                "airbyte-abctl",
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            return False

        pods_data = json.loads(result.stdout)
        pods = pods_data.get("items", [])

        if not pods:
            return False

        all_ready = all(
            pod["status"]["phase"] == "Running"
            and all(
                cs.get("ready", False)
                for cs in pod["status"].get("containerStatuses", [])
            )
            for pod in pods
        )

        return all_ready

    except Exception:
        return False


def wait_for_airbyte_ready(timeout: int = 600) -> bool:
    """Wait for Airbyte API to be ready."""
    print(f"Waiting for Airbyte API (timeout: {timeout}s)...")

    time.sleep(45)

    if check_airbyte_pods_ready():
        print("SUCCESS: Airbyte pods ready")

    end_time = time.time() + timeout
    check_interval = 15
    attempt = 0
    max_attempts_before_diagnostics = 5

    while time.time() < end_time:
        attempt += 1

        if attempt == 1 or attempt == max_attempts_before_diagnostics:
            api_url, _ = get_api_url_for_test()
            set_airbyte_api_url(api_url)
        else:
            api_url = get_airbyte_api_url()

        try:
            health_response = requests.get(f"{api_url}/health", timeout=10)
            if health_response.status_code == 200:
                password = os.environ.get("AIRBYTE_PASSWORD", BASIC_AUTH_PASSWORD)
                auth = (BASIC_AUTH_USERNAME, password)

                workspaces_response = requests.post(
                    f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
                )

                if workspaces_response.status_code == 200:
                    print("SUCCESS: Airbyte API ready")
                    return True

        except requests.RequestException:
            pass

        if attempt == max_attempts_before_diagnostics:
            diagnose_airbyte_proxy_issue()

        time.sleep(check_interval)

    print("ERROR: Timeout waiting for Airbyte API")
    return False


# =============================================================================
# Database ID Updates
# =============================================================================


def update_all_ids_atomically(
    kubeconfig_path: Path,
    id_updates: Dict[str, Dict[str, Any]],
) -> bool:
    """Update all Airbyte database IDs in a single atomic transaction."""
    if not shutil.which("kubectl") or not kubeconfig_path.exists():
        return False

    try:
        db_pod_name = "airbyte-db-0"

        sql_statements = [
            "BEGIN;",
            "ALTER TABLE connection DISABLE TRIGGER ALL;",
            "ALTER TABLE actor DISABLE TRIGGER ALL;",
        ]

        for old_id, new_id in id_updates.get("actors", {}).items():
            sql_statements.append(
                f"UPDATE actor SET id = '{new_id}' WHERE id = '{old_id}';"
            )

        for old_conn_id, conn_data in id_updates.get("connections", {}).items():
            new_conn_id = conn_data["new_id"]
            new_source_id = conn_data["new_source_id"]
            new_dest_id = conn_data["new_dest_id"]
            sql_statements.append(
                f"UPDATE connection SET id = '{new_conn_id}', "
                f"source_id = '{new_source_id}', destination_id = '{new_dest_id}' "
                f"WHERE id = '{old_conn_id}';"
            )

        sql_statements.extend(
            [
                "ALTER TABLE actor ENABLE TRIGGER ALL;",
                "ALTER TABLE connection ENABLE TRIGGER ALL;",
                "COMMIT;",
            ]
        )

        full_sql = " ".join(sql_statements)

        command = [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig_path),
            "exec",
            "-n",
            "airbyte-abctl",
            db_pod_name,
            "--",
            "psql",
            "-U",
            "airbyte",
            "-d",
            "db-airbyte",
            "-c",
            full_sql,
        ]

        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        return result.returncode == 0

    except Exception:
        return False


def update_airbyte_database_id(
    kubeconfig_path: Path, table_name: str, old_id: str, new_id: str
) -> bool:
    """Update a single Airbyte database ID using kubectl."""
    print(f"Updating {table_name}: {old_id} -> {new_id}")

    if not kubeconfig_path.exists():
        print(f"WARNING: kubeconfig not found at {kubeconfig_path}")
        return False

    try:
        kubectl_check = subprocess.run(
            ["kubectl", "version", "--client"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if kubectl_check.returncode != 0:
            print("WARNING: kubectl not available")
            return False
    except (subprocess.SubprocessError, FileNotFoundError):
        print("WARNING: kubectl not found in PATH")
        return False

    try:
        result = subprocess.run(
            [
                "kubectl",
                f"--kubeconfig={kubeconfig_path}",
                "exec",
                "-n",
                "airbyte-abctl",
                "airbyte-db-0",
                "--",
                "psql",
                "-U",
                "airbyte",
                "-d",
                "db-airbyte",
                "-c",
                f"UPDATE {table_name} SET id = '{new_id}' WHERE id = '{old_id}';",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            print(f"SUCCESS: Successfully updated {table_name} ID")
            if result.stdout:
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"WARNING: Failed to update {table_name} ID: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print(f"WARNING: Timeout updating {table_name} ID")
        return False
    except Exception as e:
        print(f"WARNING: Error updating {table_name} ID: {e}")
        return False


# =============================================================================
# Airbyte Setup Functions
# =============================================================================


def try_direct_api_setup() -> Optional[str]:
    """Verify Airbyte API is working and return workspace ID."""
    print("Verifying Airbyte API...")

    try:
        api_endpoints = [
            f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/public/v1",
            f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1",
        ]

        password = os.environ.get("AIRBYTE_PASSWORD", BASIC_AUTH_PASSWORD)
        auth = (BASIC_AUTH_USERNAME, password)

        for api_url in api_endpoints:
            workspaces_response = requests.get(
                f"{api_url}/workspaces", auth=auth, timeout=10
            )

            if workspaces_response.status_code == 200:
                workspaces_data = workspaces_response.json()
                workspaces = workspaces_data.get(
                    "data", workspaces_data.get("workspaces", [])
                )
                if workspaces:
                    workspace_id = workspaces[0].get("workspaceId")
                    print(f"SUCCESS: Workspace ID: {workspace_id}")
                    set_airbyte_api_url(api_url)
                    return workspace_id
            else:
                workspaces_response = requests.post(
                    f"{api_url}/workspaces/list", auth=auth, json={}, timeout=10
                )

                if workspaces_response.status_code == 200:
                    workspaces_data = workspaces_response.json()
                    workspaces = workspaces_data.get("workspaces", [])
                    if workspaces:
                        workspace_id = workspaces[0].get("workspaceId")
                        print(f"SUCCESS: Workspace ID: {workspace_id}")
                        set_airbyte_api_url(api_url)
                        return workspace_id

        print("ERROR: Could not find working API endpoint")
        return None

    except Exception as e:
        print(f"ERROR: Failed to get workspace ID: {str(e)}")
        traceback.print_exc()
        return None


def create_airbyte_test_setup(workspace_id: str) -> Dict[str, Optional[str]]:
    """Create PostgreSQL and MySQL sources, PostgreSQL destination, and connections."""
    print(f"Creating Airbyte resources for workspace {workspace_id}...")

    created_ids: Dict[str, Optional[str]] = {
        "workspace_id": workspace_id,
        "postgres_source_id": None,
        "postgres_dest_id": None,
    }

    try:
        api_url = f"http://{AIRBYTE_API_HOST}:{AIRBYTE_API_PORT}/api/v1"
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
            return created_ids

        source_defs = source_defs_response.json().get("sourceDefinitions", [])

        # Find PostgreSQL and MySQL source definition IDs
        postgres_source_def_id = None
        mysql_source_def_id = None

        for source_def in source_defs:
            name = source_def.get("name", "").lower()
            if "postgres" in name and not postgres_source_def_id:
                postgres_source_def_id = source_def["sourceDefinitionId"]
            elif "mysql" in name and not mysql_source_def_id:
                mysql_source_def_id = source_def["sourceDefinitionId"]

        if not postgres_source_def_id:
            print("WARNING: Could not find PostgreSQL source definition")
            return created_ids

        if not mysql_source_def_id:
            print("WARNING: Could not find MySQL source definition")
            return created_ids

        # Create PostgreSQL source
        postgres_source_config = {
            "name": "Test Postgres Source",
            "sourceDefinitionId": postgres_source_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": POSTGRES_PORT,
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
            created_ids["postgres_source_id"] = postgres_source_id
            print(f"Created PostgreSQL source: {postgres_source_id}")
        else:
            print(
                f"ERROR: Failed to create PostgreSQL source: {postgres_source_response.status_code}"
            )
            return created_ids

        # Create MySQL source
        mysql_source_config = {
            "name": "Test MySQL Source",
            "sourceDefinitionId": mysql_source_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": MYSQL_PORT,
                "database": "source_db",
                "username": "test",
                "password": "test",
                "ssl": False,
                "replication_method": {"method": "STANDARD"},
            },
        }

        mysql_source_response = requests.post(
            f"{api_url}/sources/create", auth=auth, json=mysql_source_config, timeout=10
        )

        mysql_source_id = None
        if mysql_source_response.status_code in [200, 201]:
            mysql_source_id = mysql_source_response.json()["sourceId"]
            created_ids["mysql_source_id"] = mysql_source_id
            print(f"Created MySQL source: {mysql_source_id}")
        else:
            print(
                f"WARNING: Failed to create MySQL source: {mysql_source_response.status_code}"
            )

        # Get destination definitions
        dest_defs_response = requests.post(
            f"{api_url}/destination_definitions/list", auth=auth, json={}, timeout=10
        )

        if dest_defs_response.status_code != 200:
            print(
                f"Failed to get destination definitions: {dest_defs_response.status_code}"
            )
            return created_ids

        dest_defs = dest_defs_response.json().get("destinationDefinitions", [])

        postgres_dest_def_id = None
        for dest_def in dest_defs:
            name = dest_def.get("name", "").lower()
            if "postgres" in name:
                postgres_dest_def_id = dest_def["destinationDefinitionId"]
                print(
                    f"Found PostgreSQL destination definition: {dest_def['name']} ({postgres_dest_def_id})"
                )
                break

        if not postgres_dest_def_id:
            print("WARNING: Could not find PostgreSQL destination definition")
            return created_ids

        # Create PostgreSQL destination
        print("Creating PostgreSQL destination...")
        postgres_dest_config = {
            "name": "Test Postgres Destination",
            "destinationDefinitionId": postgres_dest_def_id,
            "workspaceId": workspace_id,
            "connectionConfiguration": {
                "host": "host.docker.internal",
                "port": POSTGRES_PORT,
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
            created_ids["postgres_dest_id"] = postgres_dest_id
            print(f"Created PostgreSQL destination: {postgres_dest_id}")
        else:
            print(
                f"ERROR: Failed to create PostgreSQL destination: {postgres_dest_response.status_code}"
            )
            return created_ids

        # Discover and create connections
        _create_postgres_connection(
            api_url, auth, postgres_source_id, postgres_dest_id, created_ids
        )

        if mysql_source_id:
            _create_mysql_connection(
                api_url, auth, mysql_source_id, postgres_dest_id, created_ids
            )

        # Trigger sync jobs
        _trigger_sync_jobs(api_url, auth, created_ids)

        print(
            "\nSUCCESS: SUCCESS: All Airbyte sources, destinations, and connections created!"
        )
        print(f"PostgreSQL Source ID: {postgres_source_id}")
        print(f"MySQL Source ID: {mysql_source_id}")
        print(f"PostgreSQL Destination ID: {postgres_dest_id}")

        return created_ids

    except Exception as e:
        print(f"ERROR: Failed to create Airbyte test setup: {str(e)}")
        traceback.print_exc()
        return created_ids


def _discover_schema(
    api_url: str, auth: tuple, source_id: str, source_name: str
) -> Optional[Dict]:
    """Discover schema for a source with retries."""
    print(f"Discovering {source_name} schema...")
    discover_payload = {"sourceId": source_id}

    for attempt in range(3):
        try:
            discover_response = requests.post(
                f"{api_url}/sources/discover_schema",
                auth=auth,
                json=discover_payload,
                timeout=300,
            )

            if discover_response.status_code == 200:
                catalog_data = discover_response.json()
                sync_catalog = catalog_data.get("catalog")
                if sync_catalog and "streams" in sync_catalog:
                    for stream in sync_catalog["streams"]:
                        if "config" not in stream:
                            stream["config"] = {}
                        stream["config"]["selected"] = True
                        stream["config"]["syncMode"] = "full_refresh"
                        stream["config"]["destinationSyncMode"] = "overwrite"
                    print(
                        f"Discovered {len(sync_catalog['streams'])} {source_name} streams"
                    )
                return sync_catalog
            else:
                if attempt < 2:
                    time.sleep(30)
        except requests.exceptions.Timeout:
            if attempt < 2:
                time.sleep(30)
        except Exception:
            if attempt < 2:
                time.sleep(30)

    print(f"WARNING: {source_name} schema discovery failed, using minimal catalog")
    return {"streams": []}


def _create_postgres_connection(
    api_url: str, auth: tuple, source_id: str, dest_id: str, created_ids: Dict
) -> None:
    """Create PostgreSQL to PostgreSQL connection."""
    sync_catalog = _discover_schema(api_url, auth, source_id, "PostgreSQL")
    connection_config = {
        "name": "Postgres to Postgres Connection",
        "sourceId": source_id,
        "destinationId": dest_id,
        "status": "active",
        "scheduleType": "manual",
        "namespaceDefinition": "source",
        "namespaceFormat": "${SOURCE_NAMESPACE}",
        "prefix": "",
        "syncCatalog": sync_catalog,
    }

    connection_response = requests.post(
        f"{api_url}/connections/create", auth=auth, json=connection_config, timeout=30
    )

    if connection_response.status_code in [200, 201]:
        connection_id = connection_response.json().get("connectionId")
        created_ids["pg_to_pg_connection_id"] = connection_id
        print(f"Created Postgres-to-Postgres connection: {connection_id}")
    else:
        print(
            f"WARNING: Failed to create Postgres connection: {connection_response.status_code}"
        )


def _create_mysql_connection(
    api_url: str, auth: tuple, source_id: str, dest_id: str, created_ids: Dict
) -> None:
    """Create MySQL to PostgreSQL connection."""
    sync_catalog = _discover_schema(api_url, auth, source_id, "MySQL")

    connection_config = {
        "name": "MySQL to Postgres Connection",
        "sourceId": source_id,
        "destinationId": dest_id,
        "status": "active",
        "scheduleType": "manual",
        "namespaceDefinition": "source",
        "namespaceFormat": "${SOURCE_NAMESPACE}",
        "prefix": "",
        "syncCatalog": sync_catalog,
    }

    connection_response = requests.post(
        f"{api_url}/connections/create", auth=auth, json=connection_config, timeout=30
    )

    if connection_response.status_code in [200, 201]:
        connection_id = connection_response.json().get("connectionId")
        created_ids["mysql_to_pg_connection_id"] = connection_id
        print(f"Created MySQL-to-Postgres connection: {connection_id}")
    else:
        print(
            f"WARNING: Failed to create MySQL connection: {connection_response.status_code}"
        )


def _trigger_sync_jobs(api_url: str, auth: tuple, created_ids: Dict) -> None:
    """Trigger sync jobs for connections."""
    print("Triggering sync jobs...")

    for conn_key, conn_name in [
        ("pg_to_pg_connection_id", "Postgres-to-Postgres"),
        ("mysql_to_pg_connection_id", "MySQL-to-Postgres"),
    ]:
        conn_id = created_ids.get(conn_key)
        if conn_id:
            try:
                sync_response = requests.post(
                    f"{api_url}/connections/sync",
                    auth=auth,
                    json={"connectionId": conn_id},
                    timeout=30,
                )
                if sync_response.status_code in [200, 201]:
                    job_info = sync_response.json()
                    job_id = job_info.get("job", {}).get("id")
                    print(f"Started {conn_name} sync: {job_id}")
            except Exception:
                pass

    time.sleep(10)


def setup_airbyte_connections(test_resources_dir: Path) -> Dict[str, Optional[str]]:
    """Set up Airbyte sources, destinations, and connections for testing."""
    print("Setting up Airbyte connections...")

    workspace_id = try_direct_api_setup()

    if not workspace_id:
        return {}

    # Update workspace ID to static value
    kubeconfig = Path.home() / ".airbyte" / "abctl" / "abctl.kubeconfig"

    if update_airbyte_database_id(
        kubeconfig, "workspace", workspace_id, STATIC_WORKSPACE_ID
    ):
        workspace_id = STATIC_WORKSPACE_ID
        print(f"Using static workspace ID: {workspace_id}")
        time.sleep(5)

    # Create sources, destinations, and connections
    created_ids = create_airbyte_test_setup(workspace_id)

    # Update all IDs atomically
    print("Updating IDs to static values...")
    _update_ids_to_static(kubeconfig, created_ids)

    # Restart Airbyte server to reload IDs
    _restart_airbyte_server(kubeconfig)

    return created_ids


def _update_ids_to_static(kubeconfig: Path, created_ids: Dict) -> None:
    """Update all IDs to static values atomically."""
    postgres_source_id_val = created_ids.get("postgres_source_id")
    postgres_dest_id_val = created_ids.get("postgres_dest_id")
    mysql_source_id_val = created_ids.get("mysql_source_id")
    pg_connection_id_val = created_ids.get("pg_to_pg_connection_id")
    mysql_connection_id_val = created_ids.get("mysql_to_pg_connection_id")

    id_updates: Dict[str, Dict[str, Any]] = {"actors": {}, "connections": {}}

    if postgres_source_id_val:
        id_updates["actors"][postgres_source_id_val] = STATIC_POSTGRES_SOURCE_ID
    if mysql_source_id_val:
        id_updates["actors"][mysql_source_id_val] = STATIC_MYSQL_SOURCE_ID
    if postgres_dest_id_val:
        id_updates["actors"][postgres_dest_id_val] = STATIC_POSTGRES_DEST_ID

    if pg_connection_id_val and postgres_source_id_val and postgres_dest_id_val:
        id_updates["connections"][pg_connection_id_val] = {
            "new_id": STATIC_PG_TO_PG_CONNECTION_ID,
            "new_source_id": STATIC_POSTGRES_SOURCE_ID,
            "new_dest_id": STATIC_POSTGRES_DEST_ID,
        }
    if mysql_connection_id_val and mysql_source_id_val and postgres_dest_id_val:
        id_updates["connections"][mysql_connection_id_val] = {
            "new_id": STATIC_MYSQL_TO_PG_CONNECTION_ID,
            "new_source_id": STATIC_MYSQL_SOURCE_ID,
            "new_dest_id": STATIC_POSTGRES_DEST_ID,
        }

    if update_all_ids_atomically(kubeconfig, id_updates):
        if postgres_source_id_val:
            created_ids["postgres_source_id"] = STATIC_POSTGRES_SOURCE_ID
        if mysql_source_id_val:
            created_ids["mysql_source_id"] = STATIC_MYSQL_SOURCE_ID
        if postgres_dest_id_val:
            created_ids["postgres_dest_id"] = STATIC_POSTGRES_DEST_ID
        if pg_connection_id_val:
            created_ids["pg_to_pg_connection_id"] = STATIC_PG_TO_PG_CONNECTION_ID
        if mysql_connection_id_val:
            created_ids["mysql_to_pg_connection_id"] = STATIC_MYSQL_TO_PG_CONNECTION_ID
    else:
        print("WARNING: Atomic ID update failed, IDs may be dynamic")

    print(f"Updated all IDs: {created_ids}")


def _restart_airbyte_server(kubeconfig: Path) -> None:
    """Restart Airbyte server pod to reload static IDs."""
    print("Restarting Airbyte server...")
    try:
        restart_cmd = [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "rollout",
            "restart",
            "deployment/airbyte-abctl-server",
            "-n",
            "airbyte-abctl",
        ]
        restart_result = subprocess.run(
            restart_cmd, capture_output=True, text=True, timeout=30
        )

        if restart_result.returncode == 0:
            rollout_status_cmd = [
                "kubectl",
                "--kubeconfig",
                str(kubeconfig),
                "rollout",
                "status",
                "deployment/airbyte-abctl-server",
                "-n",
                "airbyte-abctl",
                "--timeout=120s",
            ]
            subprocess.run(
                rollout_status_cmd, capture_output=True, text=True, timeout=130
            )
            time.sleep(15)
            print("Airbyte server restarted")
    except Exception:
        pass


# =============================================================================
# Test Data Initialization
# =============================================================================


def init_test_data(test_resources_dir: Path) -> None:
    """Initialize test data in PostgreSQL and MySQL containers using SQL files."""
    print("Initializing test data...")

    try:
        postgres_init_file = test_resources_dir / "setup" / "init-test-db.sql"
        if postgres_init_file.exists():
            with open(postgres_init_file, "r") as f:
                postgres_sql = f.read()

            subprocess.run(
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
    except Exception:
        pass

    try:
        mysql_init_file = test_resources_dir / "setup" / "init-test-mysql.sql"
        if mysql_init_file.exists():
            with open(mysql_init_file, "r") as f:
                mysql_sql = f.read()

            subprocess.run(
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
    except Exception:
        pass


# =============================================================================
# abctl Management
# =============================================================================


def install_abctl(test_resources_dir: Path) -> Path:
    """Install abctl if not already available."""
    print("Installing abctl...")

    try:
        result = subprocess.run(
            ["abctl", "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print(f"abctl already installed: {result.stdout.strip()}")
            return Path("abctl")
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    print("Downloading abctl for CI environment...")
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

    version = "v0.30.1"
    download_url = f"https://github.com/airbytehq/abctl/releases/download/{version}/abctl-{version}-{os_name}-{arch}.tar.gz"

    abctl_path = test_resources_dir / "abctl"
    archive_path = test_resources_dir / f"abctl-{version}-{os_name}-{arch}.tar.gz"

    print(f"Downloading {download_url}...")
    try:
        urllib.request.urlretrieve(download_url, archive_path)

        with tarfile.open(archive_path, "r:gz") as tar:
            members = tar.getnames()
            binary_member = None
            for member in members:
                if member.endswith("/abctl") or member == "abctl":
                    binary_member = member
                    break

            if not binary_member:
                raise RuntimeError("Could not find abctl binary in archive")

            tar.extract(binary_member, test_resources_dir)

            if "/" in binary_member:
                extracted_path = test_resources_dir / binary_member
                extracted_path.rename(abctl_path)
                shutil.rmtree(test_resources_dir / binary_member.split("/")[0])

        archive_path.unlink()
        abctl_path.chmod(0o755)

        result = subprocess.run(
            [str(abctl_path), "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            raise RuntimeError(f"Downloaded abctl is not working: {result.stderr}")

        print(f"abctl {version} installed successfully for {os_name}-{arch}")
        return abctl_path

    except Exception as e:
        if archive_path.exists():
            archive_path.unlink()
        if abctl_path.exists():
            abctl_path.unlink()
        raise RuntimeError(f"Failed to download abctl: {e}") from e


def cleanup_airbyte(abctl_path: Path, test_resources_dir: Path) -> None:
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


def get_airbyte_credentials(abctl_path: Path, test_resources_dir: Path) -> None:
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


def complete_airbyte_onboarding() -> bool:
    """Complete Airbyte onboarding programmatically."""
    print("Attempting to complete Airbyte onboarding programmatically...")
    try:
        api_endpoints = [
            f"http://localhost:{AIRBYTE_API_PORT}/api/v1",
            f"http://localhost:{AIRBYTE_API_PORT}/api/public/v1",
        ]

        for api_endpoint in api_endpoints:
            try:
                print(
                    f"Trying onboarding setup at {api_endpoint}/instance_configuration/setup"
                )

                password = os.environ.get("AIRBYTE_PASSWORD", BASIC_AUTH_PASSWORD)
                auth = (BASIC_AUTH_USERNAME, password)

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
                    return True
                else:
                    print(f"Onboarding setup returned {response.status_code}")

            except requests.RequestException as e:
                print(f"Onboarding setup failed for {api_endpoint}: {e}")
                continue

        print("Could not complete onboarding setup")
        return False

    except Exception as e:
        print(f"Exception during onboarding setup: {e}")
        return False
