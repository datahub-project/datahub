"""
Unit tests for database models.
"""

from datetime import datetime, timezone

import pytest

from datahub.cloud.router.db.models import (
    DatabaseConfig,
    DataHubInstance,
    DeploymentType,
    HealthStatus,
    TenantMapping,
    UnknownTenant,
)


class TestDeploymentType:
    """Test cases for DeploymentType enum."""

    def test_deployment_type_values(self):
        """Test that DeploymentType has expected values."""
        assert DeploymentType.CLOUD.value == "cloud"
        assert DeploymentType.ON_PREMISE.value == "on_premise"
        assert DeploymentType.HYBRID.value == "hybrid"

    def test_deployment_type_from_string(self):
        """Test creating DeploymentType from string."""
        assert DeploymentType("cloud") == DeploymentType.CLOUD
        assert DeploymentType("on_premise") == DeploymentType.ON_PREMISE
        assert DeploymentType("hybrid") == DeploymentType.HYBRID

    def test_deployment_type_invalid_value(self):
        """Test that invalid DeploymentType raises ValueError."""
        with pytest.raises(ValueError):
            DeploymentType("invalid")


class TestHealthStatus:
    """Test cases for HealthStatus enum."""

    def test_health_status_values(self):
        """Test that HealthStatus has expected values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.UNKNOWN.value == "unknown"

    def test_health_status_from_string(self):
        """Test creating HealthStatus from string."""
        assert HealthStatus("healthy") == HealthStatus.HEALTHY
        assert HealthStatus("unhealthy") == HealthStatus.UNHEALTHY
        assert HealthStatus("unknown") == HealthStatus.UNKNOWN

    def test_health_status_invalid_value(self):
        """Test that invalid HealthStatus raises ValueError."""
        with pytest.raises(ValueError):
            HealthStatus("invalid")


class TestDataHubInstance:
    """Test cases for DataHubInstance dataclass."""

    def test_create_instance_minimal(self):
        """Test creating DataHubInstance with minimal required fields."""
        instance = DataHubInstance(
            id="test-1",
            name="Test Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://test.com",
        )

        assert instance.id == "test-1"
        assert instance.name == "Test Instance"
        assert instance.deployment_type == DeploymentType.CLOUD
        assert instance.url == "https://test.com"
        assert instance.is_active is True
        assert instance.is_default is False
        assert instance.health_status == HealthStatus.UNKNOWN

    def test_create_instance_full(self):
        """Test creating DataHubInstance with all fields."""
        now = datetime.now(timezone.utc).isoformat()
        instance = DataHubInstance(
            id="test-1",
            name="Test Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://test.com",
            connection_id="conn-123",
            api_token="token-456",
            health_check_url="https://test.com/health",
            is_active=False,
            is_default=True,
            max_concurrent_requests=200,
            timeout_seconds=60,
            health_status=HealthStatus.HEALTHY,
            created_at=now,
            updated_at=now,
            last_health_check=now,
        )

        assert instance.id == "test-1"
        assert instance.name == "Test Instance"
        assert instance.deployment_type == DeploymentType.CLOUD
        assert instance.url == "https://test.com"
        assert instance.connection_id == "conn-123"
        assert instance.api_token == "token-456"
        assert instance.health_check_url == "https://test.com/health"
        assert instance.is_active is False
        assert instance.is_default is True
        assert instance.max_concurrent_requests == 200
        assert instance.timeout_seconds == 60
        assert instance.health_status == HealthStatus.HEALTHY
        assert instance.created_at == now
        assert instance.updated_at == now
        assert instance.last_health_check == now

    def test_instance_default_values(self):
        """Test that DataHubInstance has correct default values."""
        instance = DataHubInstance(
            id="test-1",
            name="Test Instance",
            deployment_type=DeploymentType.CLOUD,
            url="https://test.com",
        )

        assert instance.is_active is True
        assert instance.is_default is False
        assert instance.max_concurrent_requests == 100
        assert instance.timeout_seconds == 30
        assert instance.health_status == HealthStatus.UNKNOWN


class TestTenantMapping:
    """Test cases for TenantMapping dataclass."""

    def test_create_mapping_minimal(self):
        """Test creating TenantMapping with minimal required fields."""
        mapping = TenantMapping(
            id="mapping-1", tenant_id="tenant-123", instance_id="instance-456"
        )

        assert mapping.id == "mapping-1"
        assert mapping.tenant_id == "tenant-123"
        assert mapping.instance_id == "instance-456"
        assert mapping.team_id is None
        assert mapping.created_by == "system"

    def test_create_mapping_full(self):
        """Test creating TenantMapping with all fields."""
        now = datetime.now(timezone.utc)
        mapping = TenantMapping(
            id="mapping-1",
            tenant_id="tenant-123",
            instance_id="instance-456",
            team_id="team-789",
            created_at=now,
            updated_at=now,
            created_by="test-user",
        )

        assert mapping.id == "mapping-1"
        assert mapping.tenant_id == "tenant-123"
        assert mapping.instance_id == "instance-456"
        assert mapping.team_id == "team-789"
        assert mapping.created_at == now
        assert mapping.updated_at == now
        assert mapping.created_by == "test-user"

    def test_mapping_default_values(self):
        """Test that TenantMapping has correct default values."""
        mapping = TenantMapping(
            id="mapping-1", tenant_id="tenant-123", instance_id="instance-456"
        )

        assert mapping.team_id is None
        assert mapping.created_by == "system"
        assert mapping.created_at is None
        assert mapping.updated_at is None


class TestUnknownTenant:
    """Test cases for UnknownTenant dataclass."""

    def test_create_unknown_tenant_minimal(self):
        """Test creating UnknownTenant with minimal required fields."""
        tenant = UnknownTenant(id="unknown-1", tenant_id="tenant-123")

        assert tenant.id == "unknown-1"
        assert tenant.tenant_id == "tenant-123"
        assert tenant.team_id is None
        assert tenant.first_seen is None
        assert tenant.last_seen is None
        assert tenant.event_count == 1

    def test_create_unknown_tenant_full(self):
        """Test creating UnknownTenant with all fields."""
        now = datetime.now(timezone.utc)
        tenant = UnknownTenant(
            id="unknown-1",
            tenant_id="tenant-123",
            team_id="team-456",
            first_seen=now,
            last_seen=now,
            event_count=5,
        )

        assert tenant.id == "unknown-1"
        assert tenant.tenant_id == "tenant-123"
        assert tenant.team_id == "team-456"
        assert tenant.first_seen == now
        assert tenant.last_seen == now
        assert tenant.event_count == 5

    def test_unknown_tenant_default_values(self):
        """Test that UnknownTenant has correct default values."""
        tenant = UnknownTenant(id="unknown-1", tenant_id="tenant-123")

        assert tenant.team_id is None
        assert tenant.first_seen is None
        assert tenant.last_seen is None
        assert tenant.event_count == 1


class TestDatabaseConfig:
    """Test cases for DatabaseConfig dataclass."""

    def test_create_config_sqlite(self):
        """Test creating DatabaseConfig for SQLite."""
        config = DatabaseConfig(type="sqlite", path="test.db")

        assert config.type == "sqlite"
        assert config.path == "test.db"

    def test_create_config_mysql(self):
        """Test creating DatabaseConfig for MySQL."""
        config = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_pass",
        )

        assert config.type == "mysql"
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.database == "test_db"
        assert config.username == "test_user"
        assert config.password == "test_pass"

    def test_create_config_inmemory(self):
        """Test creating DatabaseConfig for in-memory database."""
        config = DatabaseConfig(type="inmemory")

        assert config.type == "inmemory"
        assert config.path is None

    def test_config_to_dict(self):
        """Test converting DatabaseConfig to dictionary."""
        config = DatabaseConfig(type="sqlite", path="test.db")

        config_dict = config.model_dump()
        assert config_dict["type"] == "sqlite"
        assert config_dict["path"] == "test.db"
