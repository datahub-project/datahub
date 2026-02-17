"""Test configuration with compatibility layer for cross-repo testing."""

import os
import sys
import unittest.mock
from pathlib import Path
from unittest.mock import patch

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
os.environ["KAFKA_AUTOMATIONS_CONSUMER_GROUP_PREFIX"] = "test"

# Disable OTLP export in tests to prevent connection errors to missing collector
# Keep Prometheus enabled for tests that check /metrics endpoint
os.environ["OTEL_OTLP_ENABLED"] = "false"
os.environ["OTEL_TRACING_ENABLED"] = "false"
os.environ["OTEL_SYSTEM_METRICS_ENABLED"] = "false"
os.environ["SKIP_KAFKA_CONNECTIVITY_CHECK"] = "true"


# Integration test marker for tests requiring external services
integration_test = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS", "").lower() != "true",
    reason="Integration tests requiring external credentials are skipped by default. Set RUN_INTEGRATION_TESTS=true to run them.",
)


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _mock_version_filter():
    """Bypass version filtering in tests that don't set up a real GMS connection.

    filter_tools_by_version requires a live DataHubClient with a real server_config
    to check the GMS version. Most tests use mock clients without proper server_config
    attributes, which would cause TypeErrors during version comparison.

    Tests that specifically test version filtering (test_version_requirements.py)
    override this by mocking _get_server_version_info directly.
    """
    with patch(
        "datahub_integrations.mcp_integration.tool.filter_tools_by_version",
        side_effect=lambda tools: list(tools),
    ):
        yield
