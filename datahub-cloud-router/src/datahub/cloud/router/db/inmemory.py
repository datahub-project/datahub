"""
In-memory database implementation for DataHub Cloud Router.
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .interface import Database
from .models import (
    DataHubInstance,
    DeploymentType,
    TenantMapping,
    UnknownTenant,
)


class InMemoryDatabase(Database):
    """In-memory implementation for testing"""

    def __init__(self, config: dict):
        self.instances: Dict[str, DataHubInstance] = {}
        self.tenant_mappings: Dict[str, TenantMapping] = {}
        self.unknown_tenants: Dict[str, UnknownTenant] = {}

    def init_database(self) -> None:
        """Initialize in-memory database"""
        pass

    def _get_latest_mapping(self, tenant_id: str) -> Optional[TenantMapping]:
        """Get the latest tenant mapping for a given tenant ID"""
        matching_mappings = [
            mapping
            for mapping in self.tenant_mappings.values()
            if mapping.tenant_id == tenant_id
        ]
        if matching_mappings:
            # Return the latest mapping by created_at
            return max(
                matching_mappings,
                key=lambda m: m.created_at or datetime.min.replace(tzinfo=timezone.utc),
            )
        return None

    def has_mapping(self, tenant_id: str) -> Optional[str]:
        """Check if tenant has a mapping and return instance ID (latest wins)"""
        latest_mapping = self._get_latest_mapping(tenant_id)
        return latest_mapping.instance_id if latest_mapping else None

    def get_mapping(self, tenant_id: str) -> Optional[TenantMapping]:
        """Get the tenant mapping for a given tenant ID (latest wins)"""
        return self._get_latest_mapping(tenant_id)

    def create_mapping(
        self,
        tenant_id: str,
        instance_id: str,
        team_id: Optional[str] = None,
        created_by: str = "system",
    ) -> TenantMapping:
        """Create a new tenant mapping"""
        mapping = TenantMapping(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            instance_id=instance_id,
            team_id=team_id,
            created_by=created_by,
            created_at=datetime.now(timezone.utc),
        )
        self.tenant_mappings[mapping.id] = mapping
        return mapping

    def delete_mapping(self, tenant_id: str) -> bool:
        """Delete a tenant mapping"""
        keys_to_remove = [
            key
            for key, mapping in self.tenant_mappings.items()
            if mapping.tenant_id == tenant_id
        ]
        for key in keys_to_remove:
            del self.tenant_mappings[key]
        return True

    def get_instance(self, instance_id: str) -> Optional[DataHubInstance]:
        """Get DataHub instance by ID"""
        return self.instances.get(instance_id)

    def create_instance(
        self,
        name: str,
        url: str,
        deployment_type: DeploymentType = DeploymentType.CLOUD,
        **kwargs,
    ) -> DataHubInstance:
        """Create a new DataHub instance"""
        current_time = datetime.now(timezone.utc)
        instance = DataHubInstance(
            id=str(uuid.uuid4()),
            name=name,
            deployment_type=deployment_type,
            url=url,
            created_at=current_time,
            updated_at=current_time,
            **kwargs,
        )
        self.instances[instance.id] = instance
        return instance

    def update_instance(self, instance_id: str, **kwargs) -> Optional[DataHubInstance]:
        """Update a DataHub instance"""
        if instance_id not in self.instances:
            return None

        instance = self.instances[instance_id]
        for key, value in kwargs.items():
            if hasattr(instance, key):
                setattr(instance, key, value)

        instance.updated_at = datetime.now(timezone.utc)
        return instance

    def delete_instance(self, instance_id: str) -> bool:
        """Delete a DataHub instance"""
        if instance_id in self.instances:
            del self.instances[instance_id]
            return True
        return False

    def list_instances(self) -> List[DataHubInstance]:
        """List all DataHub instances"""
        return list(self.instances.values())

    def has_default_instance(self) -> bool:
        """Check if there's a default instance configured"""
        return self.get_default_instance() is not None

    def get_default_instance(self) -> Optional[DataHubInstance]:
        """Get default DataHub instance"""
        for instance in self.instances.values():
            if instance.is_default and instance.is_active:
                return instance
        return None

    def create_unknown_tenant_alert(
        self, tenant_id: str, team_id: Optional[str] = None
    ) -> UnknownTenant:
        """Create an unknown tenant alert"""
        key = f"{tenant_id}:{team_id}"
        if key in self.unknown_tenants:
            # Update existing record
            tenant = self.unknown_tenants[key]
            tenant.last_seen = datetime.now(timezone.utc)
            tenant.event_count += 1
            return tenant
        else:
            # Create new record
            tenant = UnknownTenant(
                id=str(uuid.uuid4()),
                tenant_id=tenant_id,
                team_id=team_id,
                first_seen=datetime.now(timezone.utc),
                last_seen=datetime.now(timezone.utc),
                event_count=1,
            )
            self.unknown_tenants[key] = tenant
            return tenant

    def get_unknown_tenants(self) -> List[UnknownTenant]:
        """Get all unknown tenants"""
        return list(self.unknown_tenants.values())

    def remove_unknown_tenant(self, tenant_id: str) -> bool:
        """Remove an unknown tenant"""
        keys_to_remove = [
            key
            for key in self.unknown_tenants.keys()
            if key.startswith(f"{tenant_id}:")
        ]
        for key in keys_to_remove:
            del self.unknown_tenants[key]
        return True

    def update_unknown_tenant(
        self, tenant_id: str, **kwargs
    ) -> Optional[UnknownTenant]:
        """Update an unknown tenant"""
        keys_to_update = [
            key
            for key in self.unknown_tenants.keys()
            if key.startswith(f"{tenant_id}:")
        ]

        if keys_to_update:
            key = keys_to_update[0]
            tenant = self.unknown_tenants[key]
            for attr, value in kwargs.items():
                if hasattr(tenant, attr):
                    setattr(tenant, attr, value)
            return tenant
        return None
