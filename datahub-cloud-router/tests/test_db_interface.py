"""
Unit tests for database interface and factory.
"""

import pytest

from datahub.cloud.router.db.interface import DatabaseFactory
from datahub.cloud.router.db.models import DatabaseConfig


class TestDatabaseFactory:
    """Test cases for DatabaseFactory class."""

    def test_get_database_sqlite(self):
        """Test creating SQLite database."""
        config = DatabaseConfig(type="sqlite", path="test.db")
        factory = DatabaseFactory(config)
        db = factory.get_database()

        assert db is not None
        assert hasattr(db, "create_instance")
        assert hasattr(db, "get_instance")
        assert hasattr(db, "create_mapping")

    def test_get_database_mysql(self):
        """Test creating MySQL database."""
        config = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_pass",
        )
        factory = DatabaseFactory(config)

        # Test that MySQL database creation is attempted
        # (may fail if aiomysql is not installed, which is expected)
        db = None
        try:
            db = factory.get_database()
            # If we get here, aiomysql is available
            assert db is not None
            assert hasattr(db, "create_instance")
            assert hasattr(db, "get_instance")
        except ImportError:
            # Expected if aiomysql is not installed
            pass

        # Only check attributes if db was created successfully
        if db is not None:
            assert hasattr(db, "create_mapping")
            assert hasattr(db, "get_mapping")
            assert hasattr(db, "has_mapping")
            assert hasattr(db, "create_unknown_tenant_alert")
            assert hasattr(db, "get_unknown_tenants")
            assert hasattr(db, "remove_unknown_tenant")
            assert hasattr(db, "has_default_instance")
            assert hasattr(db, "get_default_instance")

    def test_get_database_inmemory(self):
        """Test creating in-memory database."""
        config = DatabaseConfig(type="inmemory")
        factory = DatabaseFactory(config)
        db = factory.get_database()

        assert db is not None
        assert hasattr(db, "create_instance")
        assert hasattr(db, "get_instance")
        assert hasattr(db, "create_mapping")

    def test_get_database_unsupported_type(self):
        """Test that unsupported database type raises ValueError."""
        config = DatabaseConfig(type="unsupported")
        factory = DatabaseFactory(config)

        with pytest.raises(ValueError, match="Unsupported database type: unsupported"):
            factory.get_database()

    def test_get_database_with_dict_config(self):
        """Test creating database with dictionary config."""
        config_dict = {"type": "sqlite", "path": "test.db"}
        config = DatabaseConfig(**config_dict)
        factory = DatabaseFactory(config)
        db = factory.get_database()

        assert db is not None
        assert hasattr(db, "create_instance")

    def test_get_database_with_database_config_object(self):
        """Test creating database with DatabaseConfig object."""
        config = DatabaseConfig(type="sqlite", path="test.db")
        factory = DatabaseFactory(config)
        db = factory.get_database()

        assert db is not None
        assert hasattr(db, "create_instance")


class TestDatabaseInterface:
    """Test cases for Database interface methods."""

    def test_database_interface_abstract_methods(self):
        """Test that Database interface defines required abstract methods."""
        from datahub.cloud.router.db.interface import Database

        # Check that Database is an abstract base class
        assert hasattr(Database, "__abstractmethods__")

        # Check that required methods are abstract
        required_methods = [
            "create_instance",
            "get_instance",
            "list_instances",
            "get_default_instance",
            "has_default_instance",
            "create_mapping",
            "get_mapping",
            "create_unknown_tenant_alert",
            "get_unknown_tenants",
            "remove_unknown_tenant",
        ]

        for method in required_methods:
            assert method in Database.__abstractmethods__

    def test_database_factory_imports(self):
        """Test that DatabaseFactory can import all database implementations."""
        # This test ensures that all database implementations can be imported
        # without errors when the factory tries to create them

        # SQLite should be importable
        try:
            from datahub.cloud.router.db.sqlite import SQLiteDatabase

            assert SQLiteDatabase is not None
        except ImportError as e:
            pytest.fail(f"Failed to import SQLiteDatabase: {e}")

        # In-memory should be importable
        try:
            from datahub.cloud.router.db.inmemory import InMemoryDatabase

            assert InMemoryDatabase is not None
        except ImportError as e:
            pytest.fail(f"Failed to import InMemoryDatabase: {e}")

        # MySQL should be importable (but may not work without aiomysql)
        try:
            from datahub.cloud.router.db.mysql import MySQLDatabase

            assert MySQLDatabase is not None
        except ImportError:
            # MySQL import might fail if aiomysql is not installed
            # This is acceptable for testing
            pass
