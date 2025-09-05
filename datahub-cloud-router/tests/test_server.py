"""
Unit tests for the FastAPI server.
"""

from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from datahub.cloud.router.db.models import DataHubInstance, DeploymentType
from datahub.cloud.router.server import DataHubMultiTenantRouter


class TestDataHubMultiTenantRouter:
    """Test cases for DataHubMultiTenantRouter class."""

    @pytest.fixture
    def mock_router(self):
        """Create a mock router for testing."""
        router = Mock()
        router.db = Mock()
        router.default_target_url = "http://localhost:9003"
        return router

    @pytest.fixture
    def server(self, mock_router):
        """Create a server instance with mock router."""
        # Disable authentication for backward compatibility with existing tests
        return DataHubMultiTenantRouter(
            mock_router, admin_auth_enabled=False, webhook_auth_enabled=False
        )

    @pytest.fixture
    def client(self, server):
        """Create a test client for the server."""
        return TestClient(server.app)

    def test_init(self, mock_router):
        """Test server initialization."""
        server = DataHubMultiTenantRouter(mock_router)

        assert server.router == mock_router
        assert server.app is not None
        assert server.app.title == "DataHub Multi-Tenant Router"

    def test_health_check(self, client, mock_router):
        """Test health check endpoint."""
        # Mock database responses
        mock_router.db.list_instances.return_value = [
            DataHubInstance(
                id="test-1",
                name="Test Instance",
                url="https://test.com",
                deployment_type=DeploymentType.CLOUD,
                is_active=True,
            )
        ]
        mock_router.db.get_unknown_tenants.return_value = []

        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["server"] == "datahub-multi-tenant-router"
        assert data["version"] == "0.1.0"
        assert "timestamp" in data
        assert data["stats"]["total_instances"] == 1
        assert data["stats"]["active_instances"] == 1
        assert data["stats"]["unknown_tenants"] == 0
        assert data["stats"]["default_target"] == "http://localhost:9003"

    def test_root_endpoint(self, client, mock_router):
        """Test root endpoint."""
        # Mock default instance
        default_instance = DataHubInstance(
            id="default-1",
            name="Default Instance",
            url="https://default.com",
            deployment_type=DeploymentType.CLOUD,
        )
        mock_router.db.get_default_instance.return_value = default_instance

        response = client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "DataHub Multi-Tenant Router"
        assert data["version"] == "0.1.0"
        assert (
            data["description"]
            == "Multi-tenant platform integration router for DataHub"
        )
        assert data["default_instance"]["id"] == "default-1"
        assert data["default_instance"]["name"] == "Default Instance"
        assert data["default_instance"]["url"] == "https://default.com"

    def test_teams_webhook_success(self, client, mock_router):
        """Test successful Teams webhook processing."""
        webhook_data = {
            "tenantId": "test-tenant-123",
            "channelData": {"team": {"id": "test-team-456"}},
            "type": "message",
            "text": "Hello from Teams!",
        }

        # Mock router response
        mock_router.route_event = AsyncMock(return_value={"status": "success"})

        response = client.post("/public/teams/webhook", json=webhook_data)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

        # Verify router was called with correct parameters (including headers)
        mock_router.route_event.assert_called_once_with(
            webhook_data,
            "test-tenant-123",
            "test-team-456",
            {"Content-Type": "application/json"},
        )

    def test_teams_webhook_missing_tenant_id(self, client):
        """Test Teams webhook with missing tenant ID."""
        webhook_data = {"type": "message", "text": "Hello from Teams!"}

        response = client.post("/public/teams/webhook", json=webhook_data)

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "Missing tenant ID" in data["error"]

    def test_teams_webhook_router_error(self, client, mock_router):
        """Test Teams webhook when router raises an error."""
        webhook_data = {"tenantId": "test-tenant-123", "type": "message"}

        # Mock router to raise an error
        mock_router.route_event = AsyncMock(side_effect=Exception("Router error"))

        response = client.post("/public/teams/webhook", json=webhook_data)

        assert response.status_code == 500
        data = response.json()
        assert "error" in data
        assert "Error processing webhook" in data["error"]

    def test_bot_framework_message_success(self, client, mock_router):
        """Test successful Bot Framework message processing."""
        message_data = {
            "conversation": {
                "tenantId": "test-tenant-123",
                "channelData": {"team": {"id": "test-team-456"}},
            },
            "type": "message",
            "text": "Hello from Bot Framework!",
        }

        # Mock router response
        mock_router.route_event = AsyncMock(return_value={"status": "success"})

        response = client.post("/api/messages", json=message_data)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

        # Verify router was called with correct parameters
        mock_router.route_event.assert_called_once_with(
            message_data, "test-tenant-123", "test-team-456"
        )

    def test_bot_framework_message_missing_tenant_id(self, client):
        """Test Bot Framework message with missing tenant ID."""
        message_data = {"conversation": {}, "type": "message"}

        response = client.post("/api/messages", json=message_data)

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "Missing tenant ID" in data["error"]

    def test_oauth_callback_endpoint_exists(self, client):
        """Test that OAuth callback endpoint exists and responds."""
        response = client.get(
            "/public/teams/oauth/callback?code=auth_code&state=encoded_state"
        )

        # The endpoint should respond (even if with an error due to invalid state)
        assert response.status_code in [200, 400]
        data = response.json()
        assert "error" in data or "status" in data

    def test_oauth_callback_missing_parameters(self, client):
        """Test OAuth callback with missing parameters."""
        response = client.get("/public/teams/oauth/callback")

        assert response.status_code == 400
        data = response.json()
        assert "error" in data

    def test_oauth_callback_invalid_state(self, client):
        """Test OAuth callback with invalid state."""
        response = client.get(
            "/public/teams/oauth/callback?code=auth_code&state=invalid_state"
        )

        assert response.status_code == 400
        data = response.json()
        assert "error" in data

    @pytest.mark.asyncio
    async def test_auto_register_tenant_after_oauth(self, server, mock_router):
        """Test auto-registration of tenant after OAuth."""
        # Mock instance creation
        mock_instance = DataHubInstance(
            id="new-instance-1",
            name="DataHub Instance (https://test.com)",
            deployment_type=DeploymentType.CLOUD,
            url="https://test.com",
        )
        mock_router.db.get_instance.return_value = None
        mock_router.db.create_instance.return_value = mock_instance

        # Mock mapping creation
        mock_mapping = Mock()
        mock_router.db.create_mapping.return_value = mock_mapping

        await server.auto_register_tenant_after_oauth(
            "https://test.com", "new-instance-1", "test-tenant-123"
        )

        # Verify instance was created
        mock_router.db.create_instance.assert_called_once()

        # Verify mapping was created
        mock_router.db.create_mapping.assert_called_once_with(
            tenant_id="test-tenant-123",
            instance_id="new-instance-1",
            created_by="oauth_auto_registration",
        )

        # Verify unknown tenant was removed
        mock_router.db.remove_unknown_tenant.assert_called_once_with("test-tenant-123")

    @pytest.mark.asyncio
    async def test_auto_register_tenant_existing_instance(self, server, mock_router):
        """Test auto-registration when instance already exists."""
        # Mock existing instance
        existing_instance = DataHubInstance(
            id="existing-instance-1",
            name="Existing Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://test.com",
        )
        mock_router.db.get_instance.return_value = existing_instance

        # Mock mapping creation
        mock_mapping = Mock()
        mock_router.db.create_mapping.return_value = mock_mapping

        await server.auto_register_tenant_after_oauth(
            "https://test.com", "existing-instance-1", "test-tenant-123"
        )

        # Verify instance was not created
        mock_router.db.create_instance.assert_not_called()

        # Verify mapping was created with existing instance ID
        mock_router.db.create_mapping.assert_called_once_with(
            tenant_id="test-tenant-123",
            instance_id="existing-instance-1",
            created_by="oauth_auto_registration",
        )

    def test_admin_get_instances(self, client, mock_router):
        """Test admin endpoint to get all instances."""
        instances = [
            DataHubInstance(
                id="test-1",
                name="Test Instance 1",
                deployment_type=DeploymentType.CLOUD,
                url="https://test1.com",
            ),
            DataHubInstance(
                id="test-2",
                name="Test Instance 2",
                deployment_type=DeploymentType.ON_PREMISE,
                url="https://test2.com",
            ),
        ]
        mock_router.db.list_instances.return_value = instances

        response = client.get("/admin/instances")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["instances"]) == 2
        assert data["instances"][0]["id"] == "test-1"
        assert data["instances"][1]["id"] == "test-2"

    def test_admin_create_instance_success(self, client, mock_router):
        """Test admin endpoint to create instance successfully."""
        instance_data = {
            "name": "New Instance",
            "url": "https://new-instance.com",
            "deployment_type": "cloud",
        }

        # Mock instance creation
        created_instance = DataHubInstance(
            id="new-1",
            name="New Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://new-instance.com",
        )
        mock_router.db.create_instance.return_value = created_instance

        response = client.post("/admin/instances", json=instance_data)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["instance"]["id"] == "new-1"
        assert data["instance"]["name"] == "New Instance"

    def test_admin_create_instance_error(self, client, mock_router):
        """Test admin endpoint to create instance with error."""
        instance_data = {"name": "New Instance", "url": "https://new-instance.com"}

        # Mock instance creation to fail
        mock_router.db.create_instance.side_effect = Exception("Creation failed")

        response = client.post("/admin/instances", json=instance_data)

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "Error creating instance" in data["error"]

    def test_admin_get_tenant_mapping_exists(self, client, mock_router):
        """Test admin endpoint to get tenant mapping when it exists."""

        # Use a simple object with the required attributes
        class MockMapping:
            def __init__(self):
                self.id = "test-mapping-1"
                self.tenant_id = "test-tenant-123"
                self.instance_id = "test-instance-1"
                self.team_id = "test-team-456"
                self.created_by = "test"
                self.created_at = None

        mapping = MockMapping()

        # Mock the instance lookup as well
        mock_instance = DataHubInstance(
            id="test-instance-1",
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mock_router.db.get_mapping.return_value = mapping
        mock_router.db.get_instance.return_value = mock_instance

        response = client.get("/admin/tenants/test-tenant-123/mapping")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert len(data["mappings"]) == 1
        assert data["tenant_id"] == "test-tenant-123"
        assert data["mappings"][0]["id"] == "test-mapping-1"
        assert data["mappings"][0]["instance_id"] == "test-instance-1"

    def test_admin_get_tenant_mapping_not_exists(self, client, mock_router):
        """Test admin endpoint to get tenant mapping when it doesn't exist."""
        mock_router.db.get_mapping.return_value = None

        response = client.get("/admin/tenants/test-tenant-123/mapping")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["mappings"] == []

    def test_admin_create_tenant_mapping_success(self, client, mock_router):
        """Test admin endpoint to create tenant mapping successfully."""
        mapping_data = {"instance_id": "test-instance-1", "team_id": "test-team-456"}

        # Mock mapping creation
        class MockMapping:
            def __init__(self):
                self.id = "test-mapping-1"
                self.tenant_id = "test-tenant-123"
                self.instance_id = "test-instance-1"
                self.team_id = "test-team-456"
                self.created_by = "test"
                self.created_at = None

        created_mapping = MockMapping()

        mock_router.db.create_mapping.return_value = created_mapping

        response = client.post(
            "/admin/tenants/test-tenant-123/mapping", json=mapping_data
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["mapping"]["tenant_id"] == "test-tenant-123"
        assert data["mapping"]["instance_id"] == "test-instance-1"

    def test_admin_get_unknown_tenants(self, client, mock_router):
        """Test admin endpoint to get unknown tenants."""

        # Use simple objects with the required attributes
        class MockUnknownTenant:
            def __init__(self, tenant_id, team_id):
                self.id = f"unknown-{tenant_id}"
                self.tenant_id = tenant_id
                self.team_id = team_id
                self.first_seen = None
                self.last_seen = None
                self.event_count = 1

        unknown_tenants = [
            MockUnknownTenant("unknown-1", "team-1"),
            MockUnknownTenant("unknown-2", "team-2"),
        ]
        mock_router.db.get_unknown_tenants.return_value = unknown_tenants

        response = client.get("/admin/unknown-tenants")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["unknown_tenants"]) == 2

    def test_admin_routing_stats(self, client, mock_router):
        """Test admin endpoint to get routing statistics."""
        [
            DataHubInstance(
                id="test-1",
                name="Test 1",
                url="https://test1.com",
                deployment_type=DeploymentType.CLOUD,
                is_active=True,
            ),
            DataHubInstance(
                id="test-2",
                name="Test 2",
                url="https://test2.com",
                deployment_type=DeploymentType.CLOUD,
                is_active=False,
            ),
        ]

        # Use simple objects with the required attributes
        class MockUnknownTenant:
            def __init__(self, tenant_id, team_id):
                self.id = f"unknown-{tenant_id}"
                self.tenant_id = tenant_id
                self.team_id = team_id
                self.first_seen = None
                self.last_seen = None
                self.event_count = 1

        [
            MockUnknownTenant("unknown-1", "team-1"),
            MockUnknownTenant("unknown-2", "team-2"),
        ]
        DataHubInstance(
            id="default-1",
            name="Default Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://default.com",
            is_active=True,
        )

        # Mock the get_routing_stats method to avoid recursion
        mock_router.get_routing_stats.return_value = {
            "server": {
                "name": "DataHub Cloud Router",
                "version": "0.1.0",
                "status": "running",
            },
            "statistics": {
                "total_instances": 2,
                "active_instances": 1,
                "unknown_tenants": 2,
            },
            "configuration": {
                "default_instance": {
                    "id": "default-1",
                    "name": "Default Instance",
                    "url": "https://default.com",
                }
            },
        }

        response = client.get("/admin/routing-stats")

        assert response.status_code == 200
        data = response.json()
        assert data["server"]["name"] == "DataHub Cloud Router"
        assert data["statistics"]["total_instances"] == 2
        assert data["statistics"]["active_instances"] == 1
        assert data["statistics"]["unknown_tenants"] == 2
        assert data["configuration"]["default_instance"]["id"] == "default-1"
