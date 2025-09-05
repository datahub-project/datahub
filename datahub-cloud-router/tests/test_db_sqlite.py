"""
Unit tests for SQLite database implementation.
"""

import os
import sqlite3
import tempfile

import pytest

from datahub.cloud.router.db.models import (
    DeploymentType,
    HealthStatus,
)
from datahub.cloud.router.db.sqlite import SQLiteDatabase


class TestSQLiteDatabase:
    """Test cases for SQLiteDatabase."""

    @pytest.fixture
    def temp_db_file(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def sqlite_db(self, temp_db_file):
        """Create a SQLite database for testing."""
        config = {"path": temp_db_file}
        db = SQLiteDatabase(config)
        db.init_database()
        return db

    def test_init_database(self, temp_db_file):
        """Test database initialization."""
        config = {"path": temp_db_file}
        db = SQLiteDatabase(config)
        db.init_database()

        # Check that tables were created by connecting to the database
        with sqlite3.connect(temp_db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            assert "datahub_instances" in tables
            assert "tenant_instance_mappings" in tables
            assert "unknown_tenants" in tables

    def test_init_database_creates_directory(self, temp_db_file):
        """Test that database initialization creates the directory if it doesn't exist."""
        # Create a path in a non-existent directory
        db_dir = os.path.dirname(temp_db_file)
        new_db_path = os.path.join(db_dir, "new_dir", "test.db")

        config = {"path": new_db_path}
        db = SQLiteDatabase(config)
        db.init_database()

        # Check that the directory was created
        assert os.path.exists(os.path.dirname(new_db_path))

        # Cleanup
        if os.path.exists(new_db_path):
            os.unlink(new_db_path)
        if os.path.exists(os.path.dirname(new_db_path)):
            os.rmdir(os.path.dirname(new_db_path))

    def test_create_instance_success(self, sqlite_db):
        """Test creating a DataHub instance successfully."""
        result = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        assert result.name == "Test Instance"
        assert result.url == "https://test.com"
        assert result.deployment_type == DeploymentType.CLOUD
        assert result.id is not None
        assert result.is_active is True
        assert result.is_default is False
        assert result.max_concurrent_requests == 100
        assert result.timeout_seconds == 30
        assert result.health_status == HealthStatus.UNKNOWN

    def test_create_instance_with_all_fields(self, sqlite_db):
        """Test creating a DataHub instance with all fields specified."""
        result = sqlite_db.create_instance(
            name="Full Instance",
            url="https://full.com",
            deployment_type=DeploymentType.ON_PREMISE,
            connection_id="conn-123",
            api_token="token-456",
            health_check_url="https://full.com/health",
            is_active=False,
            is_default=True,
            max_concurrent_requests=50,
            timeout_seconds=60,
        )

        assert result.name == "Full Instance"
        assert result.url == "https://full.com"
        assert result.deployment_type == DeploymentType.ON_PREMISE
        assert result.connection_id == "conn-123"
        assert result.api_token == "token-456"
        assert result.health_check_url == "https://full.com/health"
        assert result.is_active is False
        assert result.is_default is True
        assert result.max_concurrent_requests == 50
        assert result.timeout_seconds == 60

    def test_get_instance_exists(self, sqlite_db):
        """Test getting an existing instance."""
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = sqlite_db.get_instance(instance.id)
        assert result is not None
        assert result.name == "Test Instance"
        assert result.url == "https://test.com"
        assert result.id == instance.id

    def test_get_instance_not_exists(self, sqlite_db):
        """Test getting a non-existent instance."""
        result = sqlite_db.get_instance("non-existent")
        assert result is None

    def test_list_instances_empty(self, sqlite_db):
        """Test listing instances when database is empty."""
        instances = sqlite_db.list_instances()
        assert instances == []

    def test_list_instances_with_data(self, sqlite_db):
        """Test listing instances with data."""
        instance1 = sqlite_db.create_instance(
            name="Test Instance 1",
            url="https://test1.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = sqlite_db.create_instance(
            name="Test Instance 2",
            url="https://test2.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        instances = sqlite_db.list_instances()
        assert len(instances) == 2
        instance_ids = [i.id for i in instances]
        assert instance1.id in instance_ids
        assert instance2.id in instance_ids

    def test_create_mapping_success(self, sqlite_db):
        """Test creating a tenant mapping successfully."""
        # Create an instance first
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = sqlite_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"
        assert result.id is not None
        assert result.created_by == "system"

    def test_create_mapping_with_created_by(self, sqlite_db):
        """Test creating a tenant mapping with custom created_by."""
        # Create an instance first
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = sqlite_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
            created_by="test_user",
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"
        assert result.created_by == "test_user"

    def test_create_mapping_without_team_id(self, sqlite_db):
        """Test creating a tenant mapping without team_id."""
        # Create an instance first
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = sqlite_db.create_mapping(
            tenant_id="test-tenant-123", instance_id=instance.id
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id is None

    def test_get_mapping_exists(self, sqlite_db):
        """Test getting an existing mapping."""
        # Create an instance and mapping
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        sqlite_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        result = sqlite_db.get_mapping("test-tenant-123")
        assert result is not None
        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"

    def test_get_mapping_not_exists(self, sqlite_db):
        """Test getting a non-existent mapping."""
        result = sqlite_db.get_mapping("non-existent")
        assert result is None

    def test_has_mapping_exists(self, sqlite_db):
        """Test checking if mapping exists."""
        # Create an instance and mapping
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        sqlite_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        result = sqlite_db.has_mapping("test-tenant-123")
        assert result == instance.id

    def test_has_mapping_not_exists(self, sqlite_db):
        """Test checking if mapping doesn't exist."""
        result = sqlite_db.has_mapping("non-existent")
        assert result is None

    def test_create_unknown_tenant_success(self, sqlite_db):
        """Test creating an unknown tenant successfully."""
        result = sqlite_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        assert result.tenant_id == "unknown-tenant-123"
        assert result.team_id == "unknown-team-456"
        assert result.id is not None
        assert result.event_count == 1

    def test_create_unknown_tenant_without_team_id(self, sqlite_db):
        """Test creating an unknown tenant without team_id."""
        result = sqlite_db.create_unknown_tenant_alert(tenant_id="unknown-tenant-123")

        assert result.tenant_id == "unknown-tenant-123"
        assert result.team_id is None
        assert result.id is not None
        assert result.event_count == 1

    def test_get_unknown_tenants_empty(self, sqlite_db):
        """Test getting unknown tenants when database is empty."""
        tenants = sqlite_db.get_unknown_tenants()
        assert tenants == []

    def test_get_unknown_tenants_with_data(self, sqlite_db):
        """Test getting unknown tenants with data."""
        sqlite_db.create_unknown_tenant_alert(tenant_id="unknown-1", team_id="team-1")
        sqlite_db.create_unknown_tenant_alert(tenant_id="unknown-2", team_id="team-2")

        tenants = sqlite_db.get_unknown_tenants()
        assert len(tenants) == 2
        tenant_ids = [t.tenant_id for t in tenants]
        assert "unknown-1" in tenant_ids
        assert "unknown-2" in tenant_ids

    def test_remove_unknown_tenant(self, sqlite_db):
        """Test removing an unknown tenant."""
        sqlite_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Verify it exists
        tenants = sqlite_db.get_unknown_tenants()
        assert len(tenants) == 1

        # Remove it
        sqlite_db.remove_unknown_tenant("unknown-tenant-123")

        # Verify it's gone
        tenants = sqlite_db.get_unknown_tenants()
        assert len(tenants) == 0

    def test_remove_unknown_tenant_not_exists(self, sqlite_db):
        """Test removing a non-existent unknown tenant."""
        # Should not raise an exception
        sqlite_db.remove_unknown_tenant("non-existent")

    def test_has_default_instance_false(self, sqlite_db):
        """Test checking for default instance when none exists."""
        result = sqlite_db.has_default_instance()
        assert result is False

    def test_has_default_instance_true(self, sqlite_db):
        """Test checking for default instance when one exists."""
        sqlite_db.create_instance(
            name="Default Instance",
            url="https://default.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        result = sqlite_db.has_default_instance()
        assert result is True

    def test_get_default_instance_exists(self, sqlite_db):
        """Test getting default instance when it exists."""
        instance = sqlite_db.create_instance(
            name="Default Instance",
            url="https://default.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        result = sqlite_db.get_default_instance()
        assert result is not None
        assert result.name == "Default Instance"
        assert result.is_default is True
        assert result.id == instance.id

    def test_get_default_instance_not_exists(self, sqlite_db):
        """Test getting default instance when none exists."""
        result = sqlite_db.get_default_instance()
        assert result is None

    def test_multiple_default_instances_handling(self, sqlite_db):
        """Test that only one default instance is returned even if multiple exist."""
        # Create multiple instances with is_default=True
        instance1 = sqlite_db.create_instance(
            name="Default Instance 1",
            url="https://default1.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )
        instance2 = sqlite_db.create_instance(
            name="Default Instance 2",
            url="https://default2.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        # Should return one of them (the first one found)
        result = sqlite_db.get_default_instance()
        assert result is not None
        assert result.is_default is True
        assert result.id in [instance1.id, instance2.id]

    def test_instance_deployment_types(self, sqlite_db):
        """Test creating instances with different deployment types."""
        sqlite_db.create_instance(
            name="Cloud Instance",
            url="https://cloud.com",
            deployment_type=DeploymentType.CLOUD,
        )

        sqlite_db.create_instance(
            name="On-Premise Instance",
            url="https://onprem.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        sqlite_db.create_instance(
            name="Hybrid Instance",
            url="https://hybrid.com",
            deployment_type=DeploymentType.HYBRID,
        )

        instances = sqlite_db.list_instances()
        assert len(instances) == 3

        deployment_types = [i.deployment_type for i in instances]
        assert DeploymentType.CLOUD in deployment_types
        assert DeploymentType.ON_PREMISE in deployment_types
        assert DeploymentType.HYBRID in deployment_types

    def test_mapping_uniqueness_constraint(self, sqlite_db):
        """Test that tenant-team combinations are unique."""
        # Create an instance
        instance = sqlite_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Create first mapping
        sqlite_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        # Try to create duplicate mapping - should fail
        with pytest.raises(sqlite3.IntegrityError):
            sqlite_db.create_mapping(
                tenant_id="test-tenant-123",
                instance_id=instance.id,
                team_id="test-team-456",
            )

    def test_unknown_tenant_update_existing(self, sqlite_db):
        """Test that unknown tenant alerts update existing records instead of creating duplicates."""
        # Create first unknown tenant
        tenant1 = sqlite_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Verify it was created
        assert tenant1.tenant_id == "unknown-tenant-123"
        assert tenant1.team_id == "unknown-team-456"
        assert tenant1.event_count == 1

        # Create "duplicate" - should update existing record
        tenant2 = sqlite_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Should be the same record with updated event count
        assert tenant2.tenant_id == "unknown-tenant-123"
        assert tenant2.team_id == "unknown-team-456"
        assert tenant2.event_count == 2

        # Verify only one record exists in database
        tenants = sqlite_db.get_unknown_tenants()
        matching_tenants = [
            t
            for t in tenants
            if t.tenant_id == "unknown-tenant-123" and t.team_id == "unknown-team-456"
        ]
        assert len(matching_tenants) == 1
        assert matching_tenants[0].event_count == 2

    def test_foreign_key_constraint(self, sqlite_db):
        """Test that mappings reference valid instances."""
        # Try to create mapping with non-existent instance
        with pytest.raises(sqlite3.IntegrityError):
            sqlite_db.create_mapping(
                tenant_id="test-tenant-123",
                instance_id="non-existent-instance",
                team_id="test-team-456",
            )

    def test_database_persistence(self, temp_db_file):
        """Test that data persists across database connections."""
        # Create database and add data
        config = {"path": temp_db_file}
        db1 = SQLiteDatabase(config)
        db1.init_database()

        instance = db1.create_instance(
            name="Persistent Instance",
            url="https://persistent.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Create new database connection
        db2 = SQLiteDatabase(config)

        # Data should still be there
        result = db2.get_instance(instance.id)
        assert result is not None
        assert result.name == "Persistent Instance"
        assert result.url == "https://persistent.com"

    def test_datetime_handling(self, sqlite_db):
        """Test that datetime fields are handled correctly."""
        instance = sqlite_db.create_instance(
            name="DateTime Test",
            url="https://datetime.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Check that created_at and updated_at are set
        assert instance.created_at is not None
        assert instance.updated_at is not None

        # Verify they are datetime objects (SQLite implementation uses datetime objects)
        from datetime import datetime

        assert isinstance(instance.created_at, datetime)
        assert isinstance(instance.updated_at, datetime)
