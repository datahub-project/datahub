"""
Unit tests for MultiTenantRouter core logic.
"""

from unittest.mock import patch

import pytest

from datahub.cloud.router.core import MultiTenantRouter
from datahub.cloud.router.db.models import DataHubInstance, DeploymentType


class TestMultiTenantRouter:
    """Test cases for MultiTenantRouter."""

    def test_init_default_config(self, router_with_mock_db):
        """Test router initialization with default config."""
        router = router_with_mock_db
        assert router.db is not None
        assert router.default_target_url == "http://localhost:9003"

    def test_init_custom_config(self):
        """Test router initialization with custom config."""
        from datahub.cloud.router.db import DatabaseConfig

        config = DatabaseConfig(type="inmemory")
        router = MultiTenantRouter(config)
        assert router.db is not None

    def test_route_event_team_specific_routing_logic(self, router_with_mock_db):
        """Test routing logic for team-specific routing."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.has_mapping.return_value = "instance-1"
        router.db.get_instance.return_value = DataHubInstance(
            id="instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Test the routing logic without HTTP calls
        result = router._get_instance_by_tenant_and_team(
            "test-tenant-123", "test-team-456"
        )

        assert result is not None
        assert result.id == "instance-1"
        router.db.has_mapping.assert_called_once_with("test-tenant-123")
        router.db.get_instance.assert_called_once_with("instance-1")

    def test_route_event_tenant_only_routing_logic(self, router_with_mock_db):
        """Test routing logic for tenant-only routing."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.has_mapping.return_value = "instance-1"
        router.db.get_instance.return_value = DataHubInstance(
            id="instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Test the routing logic without HTTP calls
        result = router._get_instance_by_tenant("test-tenant-123")

        assert result is not None
        assert result.id == "instance-1"
        router.db.has_mapping.assert_called_once_with("test-tenant-123")
        router.db.get_instance.assert_called_once_with("instance-1")

    @pytest.mark.asyncio
    async def test_route_event_no_mapping_found(
        self, router_with_mock_db, sample_webhook_event
    ):
        """Test routing event when no mapping is found."""
        router = router_with_mock_db

        # Mock the database methods to return no mapping
        router.db.has_mapping.return_value = None
        router.db.get_default_instance.return_value = None

        result = await router.route_event(
            sample_webhook_event, "test-tenant-123", "test-team-456"
        )

        assert result["status"] == "error"
        assert "No DataHub instance mapping found" in result["message"]
        assert result["tenant_id"] == "test-tenant-123"
        assert result["team_id"] == "test-team-456"

    def test_route_event_tenant_only_no_team_logic(self, router_with_mock_db):
        """Test routing logic for tenant-only routing (no team)."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.has_mapping.return_value = "instance-1"
        router.db.get_instance.return_value = DataHubInstance(
            id="instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Test the routing logic without HTTP calls
        result = router._get_instance_by_tenant("test-tenant-123")

        assert result is not None
        assert result.id == "instance-1"
        router.db.has_mapping.assert_called_once_with("test-tenant-123")
        router.db.get_instance.assert_called_once_with("instance-1")

    def test_get_instance_by_tenant_and_team(self, router_with_mock_db):
        """Test getting instance by tenant and team."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.has_mapping.return_value = "instance-1"
        router.db.get_instance.return_value = DataHubInstance(
            id="instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = router._get_instance_by_tenant_and_team(
            "test-tenant-123", "test-team-456"
        )

        assert result is not None
        assert result.id == "instance-1"
        router.db.has_mapping.assert_called_once_with("test-tenant-123")

    def test_get_instance_by_tenant(self, router_with_mock_db):
        """Test getting instance by tenant."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.has_mapping.return_value = "instance-1"
        router.db.get_instance.return_value = DataHubInstance(
            id="instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = router._get_instance_by_tenant("test-tenant-123")

        assert result is not None
        assert result.id == "instance-1"
        router.db.has_mapping.assert_called_once_with("test-tenant-123")

    def test_create_unknown_tenant_alert(self, router_with_mock_db):
        """Test creating unknown tenant alert."""
        router = router_with_mock_db

        router._create_unknown_tenant_alert("test-tenant-123", "test-team-456")

        router.db.create_unknown_tenant_alert.assert_called_once_with(
            "test-tenant-123", "test-team-456"
        )

    def test_ensure_default_instance_exists(self, router_with_mock_db):
        """Test ensuring default instance exists when it already exists."""
        router = router_with_mock_db

        # Mock that default instance exists
        router.db.has_default_instance.return_value = True

        router._ensure_default_instance()

        router.db.has_default_instance.assert_called_once()
        router.db.create_instance.assert_not_called()

    def test_ensure_default_instance_creates_new(self, router_with_mock_db):
        """Test ensuring default instance creates new when none exists."""
        router = router_with_mock_db

        # Mock that no default instance exists
        router.db.has_default_instance.return_value = False

        with patch("builtins.print"):
            router._ensure_default_instance()

        router.db.has_default_instance.assert_called_once()
        router.db.create_instance.assert_called_once()

    def test_get_routing_stats(self, router_with_mock_db):
        """Test getting routing statistics."""
        router = router_with_mock_db

        # Mock the database methods
        router.db.list_instances.return_value = [
            DataHubInstance(
                id="1",
                name="Instance 1",
                url="https://test1.com",
                deployment_type=DeploymentType.CLOUD,
                is_active=True,
            ),
            DataHubInstance(
                id="2",
                name="Instance 2",
                url="https://test2.com",
                deployment_type=DeploymentType.CLOUD,
                is_active=False,
            ),
        ]
        router.db.get_unknown_tenants.return_value = []
        router.db.get_default_instance.return_value = DataHubInstance(
            id="1",
            name="Instance 1",
            url="https://test1.com",
            deployment_type=DeploymentType.CLOUD,
            is_active=True,
        )

        stats = router.get_routing_stats()

        assert stats["total_instances"] == 2
        assert stats["active_instances"] == 1
        assert stats["default_instance"] == "Instance 1"
        assert stats["unknown_tenants_count"] == 0
        assert "timestamp" in stats
