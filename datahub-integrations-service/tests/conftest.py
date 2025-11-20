"""Test configuration with compatibility layer for cross-repo testing."""

import os
import sys
import unittest.mock
from pathlib import Path

import pytest
from datahub.testing.pytest_hooks import (  # noqa: F401
    load_golden_flags,
    pytest_addoption,
)

# === Compatibility Layer for Cross-Repo Testing ===
# This allows tests to use datahub_integrations imports in both repos
repo_root = Path(__file__).resolve().parents[1]

possible_locations = [
    # Integrations service structure
    repo_root / "src" / "datahub_integrations",
    # OSS structure
    repo_root / "src" / "mcp_server_datahub",
]

# Find which package structure exists
using_oss = False
for loc in possible_locations:
    if loc.exists() and loc.name == "mcp_server_datahub":
        using_oss = True
        break

# If in OSS repo, create datahub_integrations compatibility shim
if using_oss:
    import types

    # Create datahub_integrations package
    datahub_integrations = types.ModuleType("datahub_integrations")
    sys.modules["datahub_integrations"] = datahub_integrations

    # Create datahub_integrations.mcp submodule
    mcp_module = types.ModuleType("datahub_integrations.mcp")
    sys.modules["datahub_integrations.mcp"] = mcp_module
    datahub_integrations.mcp = mcp_module  # type: ignore[attr-defined]  # Dynamic attribute

    # Import and expose mcp_server
    from mcp_server_datahub import mcp_server

    mcp_module.mcp_server = mcp_server  # type: ignore[attr-defined]  # Dynamic attribute
    sys.modules["datahub_integrations.mcp.mcp_server"] = mcp_server

# === End Compatibility Layer ===

# Mock DataHubGraph to prevent network calls during test imports
# The app module initializes a global graph object that tries to connect to DataHub.
# We need to mock it so tests can import app.py without a running DataHub instance.
try:
    with unittest.mock.patch("datahub.ingestion.graph.client.DataHubGraph") as mock:
        os.environ["DATAHUB_GMS_PROTOCOL"] = "http"
        os.environ["DATAHUB_GMS_HOST"] = "example.com"
        os.environ["DATAHUB_GMS_PORT"] = "8080"

        # Import app module while DataHubGraph is mocked
        # This prevents the network call to localhost:8080 during test collection
        import datahub_integrations.app
except ImportError:
    # Fallback if DataHubGraph import fails
    os.environ["DATAHUB_GMS_PROTOCOL"] = "http"
    os.environ["DATAHUB_GMS_HOST"] = "example.com"
    os.environ["DATAHUB_GMS_PORT"] = "8080"

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"
