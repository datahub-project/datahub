"""
Pytest configuration and fixtures for DataHub Multi-Tenant Router tests.
"""

import asyncio
from unittest.mock import Mock

import pytest

from datahub.cloud.router import MultiTenantRouter
from datahub.cloud.router.db import DatabaseFactory
from datahub.cloud.router.db.models import (
    DataHubInstance,
    DeploymentType,
    HealthStatus,
    TenantMapping,
    UnknownTenant,
)
from tests.mysql_check import check_mysql_sync


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-mysql-tests",
        action="store_true",
        default=False,
        help="Run MySQL database tests (requires MySQL instance)",
    )
    parser.addoption(
        "--no-auto-mysql",
        action="store_true",
        default=False,
        help="Disable automatic MySQL test detection",
    )


def pytest_configure(config):
    """Configure pytest with auto-detected MySQL availability."""
    # Check if user explicitly disabled auto-detection
    if config.getoption("--no-auto-mysql"):
        return

    # Check if user already specified --run-mysql-tests
    if config.getoption("--run-mysql-tests"):
        return

    # Auto-detect MySQL availability
    mysql_available, reason = check_mysql_sync()

    if mysql_available:
        print(f"\n🔍 Auto-detected MySQL: {reason}")
        print("   Automatically enabling MySQL tests (use --no-auto-mysql to disable)")
        # Simulate the --run-mysql-tests flag being set
        config.option.run_mysql_tests = True
    else:
        print(f"\n⚠️  MySQL not available: {reason}")
        print("   MySQL tests will be skipped (use --run-mysql-tests to force enable)")


def should_run_mysql_tests(request):
    """Check if MySQL tests should be run (public helper for other test files)."""
    return request.config.getoption("--run-mysql-tests", default=False)


@pytest.fixture
def mock_db():
    """Create a mock database for testing."""
    db = Mock()

    # Mock all methods as synchronous since InMemoryDatabase is synchronous
    db.create_instance = Mock(return_value=Mock())
    db.get_instance = Mock(return_value=None)
    db.list_instances = Mock(return_value=[])
    db.has_default_instance = Mock(return_value=False)
    db.get_default_instance = Mock(return_value=None)
    db.create_mapping = Mock(return_value=Mock())
    db.get_mapping = Mock(return_value=None)
    db.has_mapping = Mock(return_value=None)
    db.create_unknown_tenant_alert = Mock(return_value=Mock())
    db.get_unknown_tenants = Mock(return_value=[])
    db.remove_unknown_tenant = Mock(return_value=True)
    db.update_instance = Mock(return_value=Mock())
    db.delete_instance = Mock(return_value=True)
    db.delete_mapping = Mock(return_value=True)
    db.update_unknown_tenant = Mock(return_value=Mock())

    return db


@pytest.fixture
def sample_instance():
    """Create a sample DataHub instance for testing."""
    return DataHubInstance(
        id="test-instance-1",
        name="Test DataHub Instance",
        deployment_type=DeploymentType.CLOUD,
        url="https://test-datahub.company.com",
        is_active=True,
        is_default=True,
        health_status=HealthStatus.HEALTHY,
    )


@pytest.fixture
def sample_mapping():
    """Create a sample tenant mapping for testing."""
    return TenantMapping(
        id="test-mapping-1",
        tenant_id="test-tenant-123",
        datahub_instance_id="test-instance-1",
        team_id="test-team-456",
        team_name="Test Team",
        is_default_for_tenant=True,
        is_active=True,
        created_by="test",
    )


@pytest.fixture
def sample_unknown_tenant():
    """Create a sample unknown tenant for testing."""
    return UnknownTenant(
        id="unknown-tenant-1",
        tenant_id="unknown-tenant-123",
        team_id="unknown-team-456",
        team_name="Unknown Team",
        first_seen="2024-01-01T00:00:00Z",
        last_seen="2024-01-01T00:00:00Z",
        event_count=1,
    )


@pytest.fixture
def router_with_mock_db(mock_db):
    """Create a router instance with a mock database."""
    router = MultiTenantRouter()
    router.db = mock_db
    return router


@pytest.fixture
def inmemory_db():
    """Create an in-memory database for integration testing."""
    return DatabaseFactory.create(db_type="inmemory")


@pytest.fixture
def sample_webhook_event():
    """Create a sample Teams webhook event for testing."""
    return {
        "tenantId": "test-tenant-123",
        "channelData": {"team": {"id": "test-team-456"}},
        "type": "message",
        "text": "Hello from Teams!",
        "from": {"id": "user-123", "name": "Test User"},
    }


@pytest.fixture
def sample_oauth_state():
    """Create a sample OAuth state for testing."""
    return {
        "url": "https://test-datahub.company.com",
        "instance_id": "test-instance-1",
        "tenant_id": "test-tenant-123",
        "flow_type": "platform_integration",
        "redirect_path": "/settings/platform-integration",
    }


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
