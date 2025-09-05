"""
Unit tests for InMemoryDatabase implementation.
"""

from datahub.cloud.router.db.models import (
    DeploymentType,
)


class TestInMemoryDatabase:
    """Test cases for InMemoryDatabase."""

    def test_init(self, inmemory_db):
        """Test database initialization."""
        assert inmemory_db.instances == {}
        assert inmemory_db.tenant_mappings == {}
        assert inmemory_db.unknown_tenants == {}

    def test_create_instance_success(self, inmemory_db):
        """Test creating a DataHub instance successfully."""
        result = inmemory_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        assert result.name == "Test Instance"
        assert result.url == "https://test.com"
        assert result.deployment_type == DeploymentType.CLOUD
        assert result.id in inmemory_db.instances
        assert len(inmemory_db.instances) == 1

    def test_create_instance_duplicate_id(self, inmemory_db):
        """Test creating an instance with duplicate ID."""
        instance1 = inmemory_db.create_instance(
            name="Test Instance 1",
            url="https://test1.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = inmemory_db.create_instance(
            name="Test Instance 2",
            url="https://test2.com",
            deployment_type=DeploymentType.CLOUD,
        )

        assert instance1.id != instance2.id
        assert len(inmemory_db.instances) == 2

    def test_get_instance_exists(self, inmemory_db):
        """Test getting an existing instance."""
        instance = inmemory_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = inmemory_db.get_instance(instance.id)
        assert result == instance

    def test_get_instance_not_exists(self, inmemory_db):
        """Test getting a non-existent instance."""
        result = inmemory_db.get_instance("non-existent")
        assert result is None

    def test_list_instances_empty(self, inmemory_db):
        """Test listing instances when database is empty."""
        instances = inmemory_db.list_instances()
        assert instances == []

    def test_list_instances_with_data(self, inmemory_db):
        """Test listing instances with data."""
        instance1 = inmemory_db.create_instance(
            name="Test Instance 1",
            url="https://test1.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = inmemory_db.create_instance(
            name="Test Instance 2",
            url="https://test2.com",
            deployment_type=DeploymentType.CLOUD,
        )

        instances = inmemory_db.list_instances()
        assert len(instances) == 2
        assert instance1 in instances
        assert instance2 in instances

    def test_has_default_instance_true(self, inmemory_db):
        """Test checking for default instance when it exists."""
        inmemory_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
            is_active=True,
        )

        result = inmemory_db.has_default_instance()
        assert result is True

    def test_has_default_instance_false(self, inmemory_db):
        """Test checking for default instance when none exists."""
        inmemory_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=False,
            is_active=True,
        )

        result = inmemory_db.has_default_instance()
        assert result is False

    def test_get_default_instance_exists(self, inmemory_db):
        """Test getting default instance when it exists."""
        instance = inmemory_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
            is_active=True,
        )

        result = inmemory_db.get_default_instance()
        assert result == instance

    def test_get_default_instance_not_exists(self, inmemory_db):
        """Test getting default instance when none exists."""
        result = inmemory_db.get_default_instance()
        assert result is None

    def test_create_mapping_success(self, inmemory_db):
        """Test creating a tenant mapping successfully."""
        mapping = inmemory_db.create_mapping(
            tenant_id="tenant-123", instance_id="instance-456"
        )

        assert mapping.tenant_id == "tenant-123"
        assert mapping.instance_id == "instance-456"
        assert mapping.id in inmemory_db.tenant_mappings
        assert len(inmemory_db.tenant_mappings) == 1

    def test_create_mapping_duplicate_id(self, inmemory_db):
        """Test creating a mapping with duplicate ID."""
        mapping1 = inmemory_db.create_mapping(
            tenant_id="tenant-123", instance_id="instance-456"
        )
        mapping2 = inmemory_db.create_mapping(
            tenant_id="tenant-789", instance_id="instance-456"
        )

        assert mapping1.id != mapping2.id
        assert len(inmemory_db.tenant_mappings) == 2

    def test_get_mapping_exists(self, inmemory_db):
        """Test getting an existing mapping."""
        mapping = inmemory_db.create_mapping(
            tenant_id="tenant-123", instance_id="instance-456"
        )

        result = inmemory_db.get_mapping("tenant-123")
        assert result == mapping

    def test_get_mapping_not_exists(self, inmemory_db):
        """Test getting a non-existent mapping."""
        result = inmemory_db.get_mapping("non-existent")
        assert result is None

    def test_has_mapping_exists(self, inmemory_db):
        """Test checking if mapping exists when it does."""
        # Create mapping synchronously
        inmemory_db.create_mapping(tenant_id="tenant-123", instance_id="instance-456")

        result = inmemory_db.has_mapping("tenant-123")
        assert result == "instance-456"

    def test_has_mapping_not_exists(self, inmemory_db):
        """Test checking if mapping exists when it doesn't."""
        result = inmemory_db.has_mapping("non-existent")
        assert result is None

    def test_create_unknown_tenant_alert_success(self, inmemory_db):
        """Test creating unknown tenant alert successfully."""
        result = inmemory_db.create_unknown_tenant_alert("tenant-123", "team-456")

        assert result.tenant_id == "tenant-123"
        assert result.team_id == "team-456"
        assert result.event_count == 1
        assert "tenant-123:team-456" in inmemory_db.unknown_tenants

    def test_get_unknown_tenants_empty(self, inmemory_db):
        """Test getting unknown tenants when database is empty."""
        tenants = inmemory_db.get_unknown_tenants()
        assert tenants == []

    def test_get_unknown_tenants_with_data(self, inmemory_db):
        """Test getting unknown tenants with data."""
        tenant1 = inmemory_db.create_unknown_tenant_alert("tenant-123", "team-456")
        tenant2 = inmemory_db.create_unknown_tenant_alert("tenant-789", "team-101")

        tenants = inmemory_db.get_unknown_tenants()
        assert len(tenants) == 2
        assert tenant1 in tenants
        assert tenant2 in tenants

    def test_remove_unknown_tenant_exists(self, inmemory_db):
        """Test removing an existing unknown tenant."""
        inmemory_db.create_unknown_tenant_alert("tenant-123", "team-456")

        result = inmemory_db.remove_unknown_tenant("tenant-123")
        assert result is True
        assert len(inmemory_db.unknown_tenants) == 0

    def test_remove_unknown_tenant_not_exists(self, inmemory_db):
        """Test removing a non-existent unknown tenant."""
        result = inmemory_db.remove_unknown_tenant("non-existent")
        assert result is True  # Method returns True even if nothing to remove
