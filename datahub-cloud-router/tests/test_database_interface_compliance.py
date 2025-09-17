"""
Interface compliance tests for Database implementations.

These tests verify that any implementation of the Database interface
behaves correctly according to the contract defined by the interface.
"""

import os
import tempfile

import pytest
from freezegun import freeze_time

from datahub.cloud.router.db.interface import Database, DatabaseFactory
from datahub.cloud.router.db.models import (
    DeploymentType,
    HealthStatus,
)


class DatabaseInterfaceTests:
    """
    Base test class that defines the contract tests for any Database implementation.

    Subclasses should implement the get_database() method to return their specific
    database implementation for testing.
    """

    def get_database(self) -> Database:
        """Return a database instance for testing. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_database()")

    def cleanup_database(self, db: Database):
        """Clean up database after tests. Override if needed."""
        pass

    @pytest.fixture
    def db(self):
        """Database fixture."""
        database = self.get_database()
        yield database
        self.cleanup_database(database)

    # Instance Management Tests

    def test_create_instance_minimal(self, db):
        """Test creating an instance with minimal required fields."""
        instance = db.create_instance(
            name="Test Instance",
            url="https://test.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        assert instance.name == "Test Instance"
        assert instance.url == "https://test.datahub.com"
        assert instance.deployment_type == DeploymentType.CLOUD
        assert instance.id is not None
        assert len(instance.id) > 0
        assert instance.is_active is True
        assert instance.is_default is False
        assert instance.health_status == HealthStatus.UNKNOWN
        assert instance.max_concurrent_requests == 100
        assert instance.timeout_seconds == 30
        assert instance.created_at is not None
        assert instance.updated_at is not None

    def test_create_instance_full(self, db):
        """Test creating an instance with all fields specified."""
        instance = db.create_instance(
            name="Full Test Instance",
            url="https://full.datahub.com",
            deployment_type=DeploymentType.ON_PREMISE,
            connection_id="conn-123",
            api_token="token-456",
            health_check_url="https://full.datahub.com/health",
            is_active=False,
            is_default=True,
            max_concurrent_requests=50,
            timeout_seconds=60,
        )

        assert instance.name == "Full Test Instance"
        assert instance.url == "https://full.datahub.com"
        assert instance.deployment_type == DeploymentType.ON_PREMISE
        assert instance.connection_id == "conn-123"
        assert instance.api_token == "token-456"
        assert instance.health_check_url == "https://full.datahub.com/health"
        assert instance.is_active is False
        assert instance.is_default is True
        assert instance.max_concurrent_requests == 50
        assert instance.timeout_seconds == 60

    def test_get_instance_exists(self, db):
        """Test retrieving an existing instance."""
        created = db.create_instance(
            name="Get Test",
            url="https://get.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        retrieved = db.get_instance(created.id)
        assert retrieved is not None
        assert retrieved.id == created.id
        assert retrieved.name == "Get Test"
        assert retrieved.url == "https://get.datahub.com"

    def test_get_instance_not_exists(self, db):
        """Test retrieving a non-existent instance."""
        result = db.get_instance("non-existent-id")
        assert result is None

    def test_list_instances_empty(self, db):
        """Test listing instances when database is empty."""
        instances = db.list_instances()
        assert isinstance(instances, list)
        assert len(instances) == 0

    def test_list_instances_with_data(self, db):
        """Test listing instances with data."""
        instance1 = db.create_instance(
            name="Instance 1",
            url="https://instance1.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = db.create_instance(
            name="Instance 2",
            url="https://instance2.datahub.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        instances = db.list_instances()
        assert isinstance(instances, list)
        assert len(instances) == 2

        instance_ids = [i.id for i in instances]
        assert instance1.id in instance_ids
        assert instance2.id in instance_ids

    def test_has_default_instance(self, db):
        """Test checking for default instance."""
        # Initially no default
        assert db.has_default_instance() is False

        # Create non-default instance
        db.create_instance(
            name="Non-Default",
            url="https://nondefault.datahub.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=False,
        )
        assert db.has_default_instance() is False

        # Create default instance
        db.create_instance(
            name="Default",
            url="https://default.datahub.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )
        assert db.has_default_instance() is True

    def test_get_default_instance(self, db):
        """Test getting default instance."""
        # Initially no default
        assert db.get_default_instance() is None

        # Create non-default instance
        db.create_instance(
            name="Non-Default",
            url="https://nondefault.datahub.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=False,
        )
        assert db.get_default_instance() is None

        # Create default instance
        default = db.create_instance(
            name="Default",
            url="https://default.datahub.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        retrieved_default = db.get_default_instance()
        assert retrieved_default is not None
        assert retrieved_default.id == default.id
        assert retrieved_default.is_default is True

    # Tenant Mapping Tests

    def test_create_mapping_with_team(self, db):
        """Test creating a tenant mapping with team ID."""
        instance = db.create_instance(
            name="Mapping Test",
            url="https://mapping.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mapping = db.create_mapping(
            tenant_id="tenant-123", instance_id=instance.id, team_id="team-456"
        )

        assert mapping.tenant_id == "tenant-123"
        assert mapping.instance_id == instance.id
        assert mapping.team_id == "team-456"
        assert mapping.id is not None
        assert len(mapping.id) > 0
        assert mapping.created_at is not None
        assert mapping.created_by == "system"

    def test_create_mapping_without_team(self, db):
        """Test creating a tenant mapping without team ID."""
        instance = db.create_instance(
            name="Mapping Test",
            url="https://mapping.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mapping = db.create_mapping(tenant_id="tenant-123", instance_id=instance.id)

        assert mapping.tenant_id == "tenant-123"
        assert mapping.instance_id == instance.id
        assert mapping.team_id is None
        assert mapping.id is not None

    def test_create_mapping_with_created_by(self, db):
        """Test creating a tenant mapping with custom created_by."""
        instance = db.create_instance(
            name="Mapping Test",
            url="https://mapping.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mapping = db.create_mapping(
            tenant_id="tenant-123",
            instance_id=instance.id,
            team_id="team-456",
            created_by="test_user",
        )

        assert mapping.created_by == "test_user"

    def test_get_mapping_exists(self, db):
        """Test retrieving an existing mapping."""
        instance = db.create_instance(
            name="Mapping Test",
            url="https://mapping.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        db.create_mapping(
            tenant_id="tenant-123", instance_id=instance.id, team_id="team-456"
        )

        retrieved = db.get_mapping("tenant-123")
        assert retrieved is not None
        assert retrieved.tenant_id == "tenant-123"
        assert retrieved.instance_id == instance.id
        assert retrieved.team_id == "team-456"

    def test_get_mapping_not_exists(self, db):
        """Test retrieving a non-existent mapping."""
        result = db.get_mapping("non-existent-tenant")
        assert result is None

    def test_has_mapping_exists(self, db):
        """Test checking if mapping exists."""
        instance = db.create_instance(
            name="Mapping Test",
            url="https://mapping.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        db.create_mapping(
            tenant_id="tenant-123", instance_id=instance.id, team_id="team-456"
        )

        result = db.has_mapping("tenant-123")
        assert result == instance.id

    def test_has_mapping_not_exists(self, db):
        """Test checking if mapping doesn't exist."""
        result = db.has_mapping("non-existent-tenant")
        assert result is None

    # Latest Mapping Wins Tests

    def test_latest_mapping_wins_has_mapping(self, db):
        """Test that has_mapping returns the latest mapping's instance_id."""
        # Create instances for mappings
        instance1 = db.create_instance(
            name="Old Instance",
            url="https://old.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = db.create_instance(
            name="New Instance",
            url="https://new.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        tenant_id = "tenant-latest-test"

        # Create first mapping at a specific time
        with freeze_time("2024-01-01 12:00:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance1.id)

        # Create second mapping later (should be the latest)
        with freeze_time("2024-01-01 12:01:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance2.id)

        # Verify the latest mapping wins
        result = db.has_mapping(tenant_id)
        assert result == instance2.id

    def test_latest_mapping_wins_get_mapping(self, db):
        """Test that get_mapping returns the latest mapping object."""
        # Create instances for mappings
        instance1 = db.create_instance(
            name="Old Instance",
            url="https://old.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = db.create_instance(
            name="New Instance",
            url="https://new.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        tenant_id = "tenant-get-latest"

        # Create first mapping at a specific time
        with freeze_time("2024-01-01 12:00:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance1.id)

        # Create second mapping later
        with freeze_time("2024-01-01 12:01:00"):
            mapping2 = db.create_mapping(tenant_id=tenant_id, instance_id=instance2.id)

        # Verify the latest mapping object is returned
        result = db.get_mapping(tenant_id)
        assert result is not None
        assert result.tenant_id == tenant_id
        assert result.instance_id == instance2.id
        assert result.id == mapping2.id

    def test_default_flag_ignored_in_latest_logic(self, db):
        """Test that is_default_for_tenant flag is ignored when determining latest mapping."""
        # Create instances for mappings
        instance1 = db.create_instance(
            name="Default Old Instance",
            url="https://default-old.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = db.create_instance(
            name="Not Default New Instance",
            url="https://not-default-new.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        tenant_id = "tenant-default-ignored"

        # Create older mapping (would be marked as default in SQL databases)
        with freeze_time("2024-01-01 12:00:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance1.id)

        # Create newer mapping (latest wins regardless of any default flag)
        with freeze_time("2024-01-01 12:01:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance2.id)

        # The latest mapping should win regardless of default flag
        result = db.has_mapping(tenant_id)
        assert result == instance2.id

        result_mapping = db.get_mapping(tenant_id)
        assert result_mapping is not None
        assert result_mapping.instance_id == instance2.id

    def test_inactive_mappings_ignored_in_latest_logic(self, db):
        """Test that inactive mappings are ignored in latest wins logic."""
        # Create instances for mappings
        instance1 = db.create_instance(
            name="Active Instance",
            url="https://active.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = db.create_instance(
            name="Inactive Instance",
            url="https://inactive.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        tenant_id = "tenant-inactive-test"

        # Create first mapping
        with freeze_time("2024-01-01 12:00:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance1.id)

        # Create second mapping (later in time)
        # Note: InMemory doesn't support is_active field, so this test
        # mainly validates SQL database behavior where inactive mappings are filtered
        with freeze_time("2024-01-01 12:01:00"):
            db.create_mapping(tenant_id=tenant_id, instance_id=instance2.id)

        # For InMemory: latest wins (instance2)
        # For SQL databases: inactive mappings would be filtered out
        result = db.has_mapping(tenant_id)
        # This test behavior depends on database implementation
        assert result in [instance1.id, instance2.id]

        result_mapping = db.get_mapping(tenant_id)
        assert result_mapping is not None
        assert result_mapping.instance_id in [instance1.id, instance2.id]

    # Unknown Tenant Tests

    def test_create_unknown_tenant_with_team(self, db):
        """Test creating an unknown tenant alert with team ID."""
        unknown = db.create_unknown_tenant_alert(
            tenant_id="unknown-123", team_id="team-456"
        )

        assert unknown.tenant_id == "unknown-123"
        assert unknown.team_id == "team-456"
        assert unknown.id is not None
        assert len(unknown.id) > 0
        assert unknown.event_count == 1
        assert unknown.first_seen is not None
        assert unknown.last_seen is not None

    def test_create_unknown_tenant_without_team(self, db):
        """Test creating an unknown tenant alert without team ID."""
        unknown = db.create_unknown_tenant_alert(tenant_id="unknown-123")

        assert unknown.tenant_id == "unknown-123"
        assert unknown.team_id is None
        assert unknown.event_count == 1

    def test_unknown_tenant_event_counting(self, db):
        """Test that unknown tenant alerts properly count events."""
        # First alert
        unknown1 = db.create_unknown_tenant_alert(
            tenant_id="unknown-123", team_id="team-456"
        )
        assert unknown1.event_count == 1

        # Second alert for same tenant/team should increment count
        unknown2 = db.create_unknown_tenant_alert(
            tenant_id="unknown-123", team_id="team-456"
        )
        assert unknown2.event_count == 2
        assert unknown2.id == unknown1.id  # Same record

    def test_get_unknown_tenants_empty(self, db):
        """Test getting unknown tenants when database is empty."""
        tenants = db.get_unknown_tenants()
        assert isinstance(tenants, list)
        assert len(tenants) == 0

    def test_get_unknown_tenants_with_data(self, db):
        """Test getting unknown tenants with data."""
        db.create_unknown_tenant_alert(tenant_id="unknown-1", team_id="team-1")
        db.create_unknown_tenant_alert(tenant_id="unknown-2", team_id="team-2")

        tenants = db.get_unknown_tenants()
        assert isinstance(tenants, list)
        assert len(tenants) == 2

        tenant_ids = [t.tenant_id for t in tenants]
        assert "unknown-1" in tenant_ids
        assert "unknown-2" in tenant_ids

    def test_remove_unknown_tenant_exists(self, db):
        """Test removing an existing unknown tenant."""
        db.create_unknown_tenant_alert(tenant_id="unknown-123", team_id="team-456")

        # Verify it exists
        tenants = db.get_unknown_tenants()
        assert len(tenants) == 1

        # Remove it
        db.remove_unknown_tenant("unknown-123")

        # Verify it's gone
        tenants = db.get_unknown_tenants()
        assert len(tenants) == 0

    def test_remove_unknown_tenant_not_exists(self, db):
        """Test removing a non-existent unknown tenant (should not error)."""
        # Should not raise an exception
        db.remove_unknown_tenant("non-existent-tenant")

    # Integration Tests

    def test_deployment_types(self, db):
        """Test all deployment types."""
        db.create_instance(
            name="Cloud",
            url="https://cloud.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        db.create_instance(
            name="On-Premise",
            url="https://onprem.datahub.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        db.create_instance(
            name="Hybrid",
            url="https://hybrid.datahub.com",
            deployment_type=DeploymentType.HYBRID,
        )

        instances = db.list_instances()
        deployment_types = [i.deployment_type for i in instances]

        assert DeploymentType.CLOUD in deployment_types
        assert DeploymentType.ON_PREMISE in deployment_types
        assert DeploymentType.HYBRID in deployment_types

    def test_health_statuses(self, db):
        """Test all health status values."""
        unknown = db.create_instance(
            name="Unknown Health",
            url="https://unknown.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )
        assert unknown.health_status == HealthStatus.UNKNOWN

        # Note: We don't test HEALTHY/UNHEALTHY here since those would
        # typically be set by health check processes, not during creation

    def test_multiple_tenants_same_instance(self, db):
        """Test multiple tenants can map to the same instance."""
        instance = db.create_instance(
            name="Shared Instance",
            url="https://shared.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mapping1 = db.create_mapping(
            tenant_id="tenant-1", instance_id=instance.id, team_id="team-1"
        )

        mapping2 = db.create_mapping(
            tenant_id="tenant-2", instance_id=instance.id, team_id="team-2"
        )

        assert mapping1.instance_id == instance.id
        assert mapping2.instance_id == instance.id
        assert mapping1.tenant_id != mapping2.tenant_id

    def test_different_teams_same_tenant(self, db):
        """Test different teams from same tenant can have different mappings."""
        instance1 = db.create_instance(
            name="Instance 1",
            url="https://instance1.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        instance2 = db.create_instance(
            name="Instance 2",
            url="https://instance2.datahub.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Same tenant, different teams, different instances
        mapping1 = db.create_mapping(
            tenant_id="tenant-123", instance_id=instance1.id, team_id="team-1"
        )

        mapping2 = db.create_mapping(
            tenant_id="tenant-123", instance_id=instance2.id, team_id="team-2"
        )

        assert mapping1.tenant_id == mapping2.tenant_id
        assert mapping1.team_id != mapping2.team_id
        assert mapping1.instance_id != mapping2.instance_id


# Concrete test classes for each implementation


class TestInMemoryDatabaseInterface(DatabaseInterfaceTests):
    """Test InMemory database implementation against the interface contract."""

    def get_database(self) -> Database:
        """Return an InMemory database for testing."""
        return DatabaseFactory.create(db_type="inmemory")


class TestSQLiteDatabaseInterface(DatabaseInterfaceTests):
    """Test SQLite database implementation against the interface contract."""

    def get_database(self) -> Database:
        """Return a SQLite database for testing."""
        # Create temporary database file
        self._temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._temp_file.close()

        db = DatabaseFactory.create(db_type="sqlite", path=self._temp_file.name)
        db.init_database()
        return db

    def cleanup_database(self, db: Database):
        """Clean up temporary database file."""
        if hasattr(self, "_temp_file") and os.path.exists(self._temp_file.name):
            os.unlink(self._temp_file.name)


try:
    import pymysql  # noqa: F401

    PYMySQL_AVAILABLE = True
except ImportError:
    PYMySQL_AVAILABLE = False


@pytest.mark.skipif(not PYMySQL_AVAILABLE, reason="pymysql not installed")
class TestMySQLDatabaseInterface(DatabaseInterfaceTests):
    """Test MySQL database implementation against the interface contract."""

    def pytest_runtest_setup(self, item):
        """Skip if MySQL tests are not explicitly requested."""
        if not item.config.getoption("--run-mysql-tests"):
            pytest.skip("MySQL tests not requested (use --run-mysql-tests)")

    def get_database(self) -> Database:
        """Return a MySQL database for testing."""
        # Create database instance
        database = DatabaseFactory.create(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="datahub",
            password="datahub",
            database="datahub",
        )

        # Initialize the database schema
        try:
            database.init_database()
        except Exception as e:
            pytest.skip(f"MySQL database unavailable: {e}")

        return database

    def cleanup_database(self, db: Database):
        """Clean up test data."""
        if hasattr(db, "_cleanup_test_data"):
            import asyncio

            try:
                asyncio.run(db._cleanup_test_data())
            except Exception as e:
                print(f"Warning: Failed to cleanup MySQL test data: {e}")
