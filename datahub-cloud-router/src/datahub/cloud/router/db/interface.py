"""
Database interface and factory for DataHub Cloud Router.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from .models import (
    DatabaseConfig,
    DataHubInstance,
    DeploymentType,
    TenantMapping,
    UnknownTenant,
)


class Database(ABC):
    """Abstract database interface for DataHub Cloud Router."""

    @abstractmethod
    def init_database(self) -> None:
        """Initialize the database schema."""
        pass

    @abstractmethod
    def has_mapping(self, tenant_id: str) -> Optional[str]:
        """Check if a tenant has a mapping and return the instance ID."""
        pass

    @abstractmethod
    def get_mapping(self, tenant_id: str) -> Optional[TenantMapping]:
        """Get the tenant mapping for a given tenant ID."""
        pass

    @abstractmethod
    def create_mapping(
        self,
        tenant_id: str,
        instance_id: str,
        team_id: Optional[str] = None,
        created_by: str = "system",
    ) -> TenantMapping:
        """Create a new tenant mapping."""
        pass

    @abstractmethod
    def delete_mapping(self, tenant_id: str) -> bool:
        """Delete a tenant mapping."""
        pass

    @abstractmethod
    def get_instance(self, instance_id: str) -> Optional[DataHubInstance]:
        """Get a DataHub instance by ID."""
        pass

    @abstractmethod
    def create_instance(
        self,
        name: str,
        url: str,
        deployment_type: DeploymentType = DeploymentType.CLOUD,
        **kwargs,
    ) -> DataHubInstance:
        """Create a new DataHub instance."""
        pass

    @abstractmethod
    def update_instance(self, instance_id: str, **kwargs) -> Optional[DataHubInstance]:
        """Update a DataHub instance."""
        pass

    @abstractmethod
    def delete_instance(self, instance_id: str) -> bool:
        """Delete a DataHub instance."""
        pass

    @abstractmethod
    def list_instances(self) -> List[DataHubInstance]:
        """List all DataHub instances."""
        pass

    @abstractmethod
    def has_default_instance(self) -> bool:
        """Check if a default instance exists."""
        pass

    @abstractmethod
    def get_default_instance(self) -> Optional[DataHubInstance]:
        """Get the default DataHub instance."""
        pass

    @abstractmethod
    def create_unknown_tenant_alert(
        self, tenant_id: str, team_id: Optional[str] = None
    ) -> UnknownTenant:
        """Create an unknown tenant alert."""
        pass

    @abstractmethod
    def get_unknown_tenants(self) -> List[UnknownTenant]:
        """Get all unknown tenants."""
        pass

    @abstractmethod
    def remove_unknown_tenant(self, tenant_id: str) -> bool:
        """Remove an unknown tenant."""
        pass

    @abstractmethod
    def update_unknown_tenant(
        self, tenant_id: str, **kwargs
    ) -> Optional[UnknownTenant]:
        """Update an unknown tenant."""
        pass


class DatabaseFactory:
    """Factory for creating database instances."""

    def __init__(self, config: DatabaseConfig):
        self.config = config

    def get_database(self) -> Database:
        """Get a database instance based on configuration."""
        if self.config.type == "inmemory":
            from .inmemory import InMemoryDatabase

            return InMemoryDatabase(self.config.model_dump())
        elif self.config.type == "sqlite":
            from .sqlite import SQLiteDatabase

            return SQLiteDatabase(self.config.model_dump())
        elif self.config.type == "mysql":
            from .mysql import MySQLDatabase

            return MySQLDatabase(self.config.model_dump())
        else:
            raise ValueError(f"Unsupported database type: {self.config.type}")

    @classmethod
    def create(cls, db_type: str = "sqlite", **kwargs) -> Database:
        """Create a database instance with the given type and configuration."""
        config = DatabaseConfig(type=db_type, **kwargs)
        factory = cls(config)
        return factory.get_database()
