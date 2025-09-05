"""
Unit tests for MySQL database implementation.

These tests are skipped by default and require a running MySQL instance.
To run these tests manually:

1. Start a MySQL instance on localhost:3306
2. Create a database named 'datahub_router_test'
3. Use username 'datahub' and password 'datahub'
4. Run: pytest tests/test_db_mysql.py -v --run-mysql-tests

Example MySQL setup:
```sql
CREATE DATABASE datahub_router_test;
CREATE USER 'datahub'@'localhost' IDENTIFIED BY 'datahub';
GRANT ALL PRIVILEGES ON datahub_router_test.* TO 'datahub'@'localhost';
FLUSH PRIVILEGES;
```
"""

from datetime import datetime

import pytest

from datahub.cloud.router.db.models import (
    DeploymentType,
    HealthStatus,
)


def mysql_available():
    """Check if MySQL is available for testing."""
    try:
        import aiomysql  # noqa: F401

        return True
    except ImportError:
        return False


def should_run_mysql_tests(request):
    """Check if MySQL tests should be run."""
    # Import here to avoid circular imports
    from tests.conftest import should_run_mysql_tests as conftest_helper

    return conftest_helper(request)


@pytest.mark.skipif(
    not mysql_available(),
    reason="aiomysql not available. Install with: pip install aiomysql",
)
class TestMySQLDatabase:
    """Test cases for MySQLDatabase."""

    @pytest.fixture(autouse=True)
    def skip_if_mysql_disabled(self, request):
        """Skip tests if MySQL tests are disabled."""
        if not should_run_mysql_tests(request):
            pytest.skip("MySQL tests disabled. Use --run-mysql-tests to enable")

    def _get_mysql_database(self):
        """Get MySQL database class, handling import errors."""
        try:
            from datahub.cloud.router.db.mysql import MySQLDatabase

            return MySQLDatabase
        except ImportError:
            pytest.skip("aiomysql not available. Install with: pip install aiomysql")

    @pytest.fixture
    def mysql_config(self):
        """MySQL configuration for testing."""
        return {
            "host": "localhost",
            "port": 3306,
            "database": "datahub",  # Use existing datahub database
            "username": "datahub",
            "password": "datahub",
            "charset": "utf8mb4",
            "autocommit": True,
        }

    @pytest.fixture
    def mysql_db(self, mysql_config):
        """Create a MySQL database for testing."""
        MySQLDatabase = self._get_mysql_database()

        # Create the database instance
        db = MySQLDatabase(mysql_config)
        db.init_database()  # Create tables for clean state
        yield db
        # Cleanup - drop all tables and close connection
        try:
            db._cleanup_test_data()
        finally:
            db.close()

    def test_init_database(self, mysql_db):
        """Test database initialization."""
        # Check that tables were created
        with mysql_db._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                results = cursor.fetchall()
                # Handle both tuple and dict cursors
                if results and isinstance(results[0], dict):
                    tables = [list(row.values())[0] for row in results]
                else:
                    tables = [row[0] for row in results]

                assert "datahub_instances" in tables
                assert "tenant_instance_mappings" in tables
                assert "unknown_tenants" in tables

    def test_create_instance_success(self, mysql_db):
        """Test creating a DataHub instance successfully."""
        result = mysql_db.create_instance(
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

    def test_create_instance_with_all_fields(self, mysql_db):
        """Test creating a DataHub instance with all fields specified."""
        result = mysql_db.create_instance(
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

    def test_get_instance_exists(self, mysql_db):
        """Test getting an existing instance."""
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = mysql_db.get_instance(instance.id)
        assert result is not None
        assert result.name == "Test Instance"
        assert result.url == "https://test.com"
        assert result.id == instance.id

    def test_get_instance_not_exists(self, mysql_db):
        """Test getting a non-existent instance."""
        result = mysql_db.get_instance("non-existent")
        assert result is None

    def test_list_instances_empty(self, mysql_db):
        """Test listing instances when database is empty."""
        instances = mysql_db.list_instances()
        assert instances == []

    def test_list_instances_with_data(self, mysql_db):
        """Test listing instances with data."""
        instance1 = mysql_db.create_instance(
            name="Test Instance 1",
            url="https://test1.com",
            deployment_type=DeploymentType.CLOUD,
        )
        instance2 = mysql_db.create_instance(
            name="Test Instance 2",
            url="https://test2.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        instances = mysql_db.list_instances()
        assert len(instances) == 2
        instance_ids = [i.id for i in instances]
        assert instance1.id in instance_ids
        assert instance2.id in instance_ids

    def test_create_mapping_success(self, mysql_db):
        """Test creating a tenant mapping successfully."""
        # Create an instance first
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = mysql_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"
        assert result.id is not None
        assert result.created_by == "system"

    def test_create_mapping_with_created_by(self, mysql_db):
        """Test creating a tenant mapping with custom created_by."""
        # Create an instance first
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = mysql_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
            created_by="test_user",
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"
        assert result.created_by == "test_user"

    def test_create_mapping_without_team_id(self, mysql_db):
        """Test creating a tenant mapping without team_id."""
        # Create an instance first
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        result = mysql_db.create_mapping(
            tenant_id="test-tenant-123", instance_id=instance.id
        )

        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id is None

    def test_get_mapping_exists(self, mysql_db):
        """Test getting an existing mapping."""
        # Create an instance and mapping
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mysql_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        result = mysql_db.get_mapping("test-tenant-123")
        assert result is not None
        assert result.tenant_id == "test-tenant-123"
        assert result.instance_id == instance.id
        assert result.team_id == "test-team-456"

    def test_get_mapping_not_exists(self, mysql_db):
        """Test getting a non-existent mapping."""
        result = mysql_db.get_mapping("non-existent")
        assert result is None

    def test_has_mapping_exists(self, mysql_db):
        """Test checking if mapping exists."""
        # Create an instance and mapping
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mysql_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        result = mysql_db.has_mapping("test-tenant-123")  # Remove await - this is sync
        assert result == instance.id

    def test_has_mapping_not_exists(self, mysql_db):
        """Test checking if mapping doesn't exist."""
        result = mysql_db.has_mapping("non-existent")  # Remove await - this is sync
        assert result is None

    def test_create_unknown_tenant_success(self, mysql_db):
        """Test creating an unknown tenant successfully."""
        result = mysql_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        assert result.tenant_id == "unknown-tenant-123"
        assert result.team_id == "unknown-team-456"
        assert result.id is not None
        assert result.event_count == 1

    def test_create_unknown_tenant_without_team_id(self, mysql_db):
        """Test creating an unknown tenant without team_id."""
        result = mysql_db.create_unknown_tenant_alert(tenant_id="unknown-tenant-123")

        assert result.tenant_id == "unknown-tenant-123"
        assert result.team_id is None
        assert result.id is not None
        assert result.event_count == 1

    def test_get_unknown_tenants_empty(self, mysql_db):
        """Test getting unknown tenants when database is empty."""
        tenants = mysql_db.get_unknown_tenants()
        assert tenants == []

    def test_get_unknown_tenants_with_data(self, mysql_db):
        """Test getting unknown tenants with data."""
        mysql_db.create_unknown_tenant_alert(tenant_id="unknown-1", team_id="team-1")
        mysql_db.create_unknown_tenant_alert(tenant_id="unknown-2", team_id="team-2")

        tenants = mysql_db.get_unknown_tenants()
        assert len(tenants) == 2
        tenant_ids = [t.tenant_id for t in tenants]
        assert "unknown-1" in tenant_ids
        assert "unknown-2" in tenant_ids

    def test_remove_unknown_tenant(self, mysql_db):
        """Test removing an unknown tenant."""
        mysql_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Verify it exists
        tenants = mysql_db.get_unknown_tenants()
        assert len(tenants) == 1

        # Remove it
        mysql_db.remove_unknown_tenant("unknown-tenant-123")

        # Verify it's gone
        tenants = mysql_db.get_unknown_tenants()
        assert len(tenants) == 0

    def test_remove_unknown_tenant_not_exists(self, mysql_db):
        """Test removing a non-existent unknown tenant."""
        # Should not raise an exception
        mysql_db.remove_unknown_tenant("non-existent")

    def test_has_default_instance_false(self, mysql_db):
        """Test checking for default instance when none exists."""
        result = mysql_db.has_default_instance()
        assert result is False

    def test_has_default_instance_true(self, mysql_db):
        """Test checking for default instance when one exists."""
        mysql_db.create_instance(
            name="Default Instance",
            url="https://default.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        result = mysql_db.has_default_instance()
        assert result is True

    def test_get_default_instance_exists(self, mysql_db):
        """Test getting default instance when it exists."""
        instance = mysql_db.create_instance(
            name="Default Instance",
            url="https://default.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        result = mysql_db.get_default_instance()
        assert result is not None
        assert result.name == "Default Instance"
        assert result.is_default is True
        assert result.id == instance.id

    def test_get_default_instance_not_exists(self, mysql_db):
        """Test getting default instance when none exists."""
        result = mysql_db.get_default_instance()
        assert result is None

    def test_multiple_default_instances_handling(self, mysql_db):
        """Test that only one default instance is returned even if multiple exist."""
        # Create multiple instances with is_default=True
        instance1 = mysql_db.create_instance(
            name="Default Instance 1",
            url="https://default1.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )
        instance2 = mysql_db.create_instance(
            name="Default Instance 2",
            url="https://default2.com",
            deployment_type=DeploymentType.CLOUD,
            is_default=True,
        )

        # Should return one of them (the first one found)
        result = mysql_db.get_default_instance()
        assert result is not None
        assert result.is_default is True
        assert result.id in [instance1.id, instance2.id]

    def test_instance_deployment_types(self, mysql_db):
        """Test creating instances with different deployment types."""
        mysql_db.create_instance(
            name="Cloud Instance",
            url="https://cloud.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mysql_db.create_instance(
            name="On-Premise Instance",
            url="https://onprem.com",
            deployment_type=DeploymentType.ON_PREMISE,
        )

        mysql_db.create_instance(
            name="Hybrid Instance",
            url="https://hybrid.com",
            deployment_type=DeploymentType.HYBRID,
        )

        instances = mysql_db.list_instances()
        assert len(instances) == 3

        deployment_types = [i.deployment_type for i in instances]
        assert DeploymentType.CLOUD in deployment_types
        assert DeploymentType.ON_PREMISE in deployment_types
        assert DeploymentType.HYBRID in deployment_types

    def test_mapping_uniqueness_constraint(self, mysql_db):
        """Test that tenant-team combinations are unique."""
        # Create an instance
        instance = mysql_db.create_instance(
            name="Test Instance",
            url="https://test.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Create first mapping
        mysql_db.create_mapping(
            tenant_id="test-tenant-123",
            instance_id=instance.id,
            team_id="test-team-456",
        )

        # Try to create duplicate mapping - should fail
        with pytest.raises(
            (ValueError, Exception)
        ):  # MySQL will raise an integrity error
            mysql_db.create_mapping(
                tenant_id="test-tenant-123",
                instance_id=instance.id,
                team_id="test-team-456",
            )

    def test_unknown_tenant_update_existing(self, mysql_db):
        """Test that unknown tenant alerts update existing records instead of creating duplicates."""
        # Create first unknown tenant
        tenant1 = mysql_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Verify it was created
        assert tenant1.tenant_id == "unknown-tenant-123"
        assert tenant1.team_id == "unknown-team-456"
        assert tenant1.event_count == 1

        # Create "duplicate" - should update existing record
        tenant2 = mysql_db.create_unknown_tenant_alert(
            tenant_id="unknown-tenant-123", team_id="unknown-team-456"
        )

        # Should be the same record with updated event count
        assert tenant2.tenant_id == "unknown-tenant-123"
        assert tenant2.team_id == "unknown-team-456"
        assert tenant2.event_count == 2

        # Verify only one record exists in database
        tenants = mysql_db.get_unknown_tenants()
        matching_tenants = [
            t
            for t in tenants
            if t.tenant_id == "unknown-tenant-123" and t.team_id == "unknown-team-456"
        ]
        assert len(matching_tenants) == 1
        assert matching_tenants[0].event_count == 2

    def test_foreign_key_constraint(self, mysql_db):
        """Test that mappings reference valid instances."""
        # Try to create mapping with non-existent instance
        with pytest.raises(
            (ValueError, Exception)
        ):  # MySQL will raise an integrity error
            mysql_db.create_mapping(
                tenant_id="test-tenant-123",
                instance_id="non-existent-instance",
                team_id="test-team-456",
            )

    def test_database_persistence(self, mysql_config):
        """Test that data persists across database connections."""
        # Create database and add data
        MySQLDatabase = self._get_mysql_database()
        db1 = MySQLDatabase(mysql_config)
        db1.init_database()

        instance = db1.create_instance(
            name="Persistent Instance",
            url="https://persistent.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Create new database connection
        db2 = MySQLDatabase(mysql_config)

        # Data should still be there
        result = db2.get_instance(instance.id)
        assert result is not None
        assert result.name == "Persistent Instance"
        assert result.url == "https://persistent.com"

        # Cleanup
        db1._cleanup_test_data()

    def test_datetime_handling(self, mysql_db):
        """Test that datetime fields are handled correctly."""
        instance = mysql_db.create_instance(
            name="DateTime Test",
            url="https://datetime.com",
            deployment_type=DeploymentType.CLOUD,
        )

        # Check that created_at and updated_at are set
        assert instance.created_at is not None
        assert instance.updated_at is not None

        # Verify they are datetime objects
        assert isinstance(instance.created_at, datetime)
        assert isinstance(instance.updated_at, datetime)

    def test_connection_pool_handling(self, mysql_db):
        """Test that connection pool is properly managed."""
        # Test multiple concurrent operations
        # Test creating multiple instances
        instances = []
        for i in range(5):
            instance = mysql_db.create_instance(
                name=f"Concurrent Instance {i}",
                url=f"https://concurrent{i}.com",
                deployment_type=DeploymentType.CLOUD,
            )
            instances.append(instance)
        assert len(instances) == 5

        # Verify all instances were created
        all_instances = mysql_db.list_instances()
        assert len(all_instances) == 5

    def test_error_handling(self, mysql_db):
        """Test error handling for invalid operations."""
        # Test with invalid deployment type (should be handled gracefully)
        with pytest.raises((ValueError, TypeError, Exception)):
            mysql_db.create_instance(
                name="Invalid Instance",
                url="https://invalid.com",
                deployment_type="invalid_type",  # This should fail
            )

    def test_cleanup_method(self, mysql_db):
        """Test the cleanup method for test data."""
        # Create some test data
        instance = mysql_db.create_instance(
            name="Cleanup Test",
            url="https://cleanup.com",
            deployment_type=DeploymentType.CLOUD,
        )

        mysql_db.create_mapping(tenant_id="cleanup-tenant", instance_id=instance.id)

        mysql_db.create_unknown_tenant_alert(tenant_id="cleanup-unknown")

        # Verify data exists
        instances = mysql_db.list_instances()
        assert len(instances) >= 1

        # Cleanup (this drops all tables)
        mysql_db._cleanup_test_data()

        # Recreate tables to verify they're empty
        mysql_db.init_database()

        # Verify data is gone
        instances = mysql_db.list_instances()
        assert len(instances) == 0
