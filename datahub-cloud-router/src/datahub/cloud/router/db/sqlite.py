"""
SQLite database implementation for DataHub Cloud Router.
"""

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .interface import Database
from .models import (
    DataHubInstance,
    DeploymentType,
    HealthStatus,
    TenantMapping,
    UnknownTenant,
)


class SQLiteDatabase(Database):
    """SQLite implementation of the database interface"""

    def __init__(self, config: dict):
        self.db_path = config.get("path", ".dev/router.db")
        self.init_database()

    def init_database(self) -> None:
        """Initialize SQLite database with required schema"""
        # Ensure the directory exists
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                -- DataHub instance registry
                CREATE TABLE IF NOT EXISTS datahub_instances (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    deployment_type TEXT NOT NULL CHECK (deployment_type IN ('cloud', 'on_premise', 'hybrid')),
                    url TEXT,
                    connection_id TEXT,
                    api_token TEXT,
                    health_check_url TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    is_default BOOLEAN DEFAULT 0,
                    max_concurrent_requests INTEGER DEFAULT 100,
                    timeout_seconds INTEGER DEFAULT 30,
                    health_status TEXT DEFAULT 'unknown' CHECK (health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown')),
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_health_check TEXT
                );
                
                -- Tenant to DataHub instance mappings
                CREATE TABLE IF NOT EXISTS tenant_instance_mappings (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    datahub_instance_id TEXT NOT NULL,
                    team_id TEXT,
                    team_name TEXT,
                    is_default_for_tenant BOOLEAN DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT,
                    FOREIGN KEY (datahub_instance_id) REFERENCES datahub_instances(id),
                    UNIQUE(tenant_id, team_id)
                );
                
                -- Unknown tenant tracking
                CREATE TABLE IF NOT EXISTS unknown_tenants (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    team_id TEXT,
                    team_name TEXT,
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    event_count INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'pending_review' CHECK (status IN ('pending_review', 'assigned', 'ignored')),
                    assigned_instance_id TEXT,
                    UNIQUE(tenant_id, team_id)
                );
                
                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_tenant_mappings_tenant_id ON tenant_instance_mappings(tenant_id);
                CREATE INDEX IF NOT EXISTS idx_tenant_mappings_instance_id ON tenant_instance_mappings(datahub_instance_id);
                CREATE INDEX IF NOT EXISTS idx_instances_deployment_type ON datahub_instances(deployment_type);
                CREATE INDEX IF NOT EXISTS idx_instances_is_active ON datahub_instances(is_active);
                CREATE INDEX IF NOT EXISTS idx_instances_is_default ON datahub_instances(is_default);
                CREATE INDEX IF NOT EXISTS idx_unknown_tenants_status ON unknown_tenants(status);
            """
            )

    def _row_to_instance(self, row: sqlite3.Row) -> DataHubInstance:
        """Convert database row to DataHubInstance"""
        return DataHubInstance(
            id=row["id"],
            name=row["name"],
            deployment_type=DeploymentType(row["deployment_type"]),
            url=row["url"],
            connection_id=row["connection_id"],
            api_token=row["api_token"],
            health_check_url=row["health_check_url"],
            is_active=bool(row["is_active"]),
            is_default=bool(row["is_default"]),
            max_concurrent_requests=row["max_concurrent_requests"],
            timeout_seconds=row["timeout_seconds"],
            health_status=HealthStatus(row["health_status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_health_check=row["last_health_check"],
        )

    def _row_to_mapping(self, row: sqlite3.Row) -> TenantMapping:
        """Convert database row to TenantMapping"""
        return TenantMapping(
            id=row["id"],
            tenant_id=row["tenant_id"],
            instance_id=row["datahub_instance_id"],
            team_id=row["team_id"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_unknown_tenant(self, row) -> UnknownTenant:
        """Convert database row to UnknownTenant"""
        # Handle both Row objects and tuples
        if hasattr(row, "__getitem__") and callable(getattr(row, "__getitem__", None)):
            # Row object
            return UnknownTenant(
                id=row["id"],
                tenant_id=row["tenant_id"],
                team_id=row["team_id"],
                first_seen=row["first_seen"],
                last_seen=row["last_seen"],
                event_count=row["event_count"],
            )
        else:
            # Tuple - assume order: id, tenant_id, team_id, team_name, first_seen, last_seen, event_count, status, assigned_instance_id
            return UnknownTenant(
                id=row[0],
                tenant_id=row[1],
                team_id=row[2],
                first_seen=row[4],
                last_seen=row[5],
                event_count=row[6],
            )

    def has_mapping(self, tenant_id: str) -> Optional[str]:
        """Check if tenant has a mapping and return instance ID (latest wins)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT datahub_instance_id FROM tenant_instance_mappings 
                WHERE tenant_id = ? AND is_active = 1
                ORDER BY created_at DESC
                LIMIT 1
            """,
                (tenant_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def get_mapping(self, tenant_id: str) -> Optional[TenantMapping]:
        """Get the tenant mapping for a given tenant ID (latest wins)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM tenant_instance_mappings 
                WHERE tenant_id = ? AND is_active = 1
                ORDER BY created_at DESC
                LIMIT 1
            """,
                (tenant_id,),
            )
            row = cursor.fetchone()
            return self._row_to_mapping(row) if row else None

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

        with sqlite3.connect(self.db_path) as conn:
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                INSERT INTO tenant_instance_mappings (
                    id, tenant_id, datahub_instance_id, team_id, created_at, created_by,
                    is_default_for_tenant, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    mapping.id,
                    mapping.tenant_id,
                    mapping.instance_id,
                    mapping.team_id,
                    mapping.created_at,
                    mapping.created_by,
                    1,  # is_default_for_tenant
                    1,  # is_active
                ),
            )

        return mapping

    def delete_mapping(self, tenant_id: str) -> bool:
        """Delete a tenant mapping"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM tenant_instance_mappings WHERE tenant_id = ?",
                    (tenant_id,),
                )
                return True
        except Exception as e:
            print(f"Error deleting mapping: {e}")
            return False

    def get_instance(self, instance_id: str) -> Optional[DataHubInstance]:
        """Get DataHub instance by ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM datahub_instances WHERE id = ?", (instance_id,)
            )
            row = cursor.fetchone()
            return self._row_to_instance(row) if row else None

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

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO datahub_instances (
                    id, name, deployment_type, url, connection_id, api_token, health_check_url,
                    is_active, is_default, max_concurrent_requests, timeout_seconds, health_status,
                    created_at, updated_at, last_health_check
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    instance.id,
                    instance.name,
                    instance.deployment_type.value,
                    instance.url,
                    instance.connection_id,
                    instance.api_token,
                    instance.health_check_url,
                    instance.is_active,
                    instance.is_default,
                    instance.max_concurrent_requests,
                    instance.timeout_seconds,
                    instance.health_status.value,
                    instance.created_at,
                    instance.updated_at,
                    instance.last_health_check,
                ),
            )

        return instance

    def update_instance(self, instance_id: str, **kwargs) -> Optional[DataHubInstance]:
        """Update a DataHub instance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Build update query dynamically
                update_fields = []
                values = []
                for key, value in kwargs.items():
                    if key in ["deployment_type", "health_status"]:
                        update_fields.append(f"{key} = ?")
                        values.append(value.value if hasattr(value, "value") else value)
                    else:
                        update_fields.append(f"{key} = ?")
                        values.append(value)

                if update_fields:
                    update_fields.append("updated_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                    values.append(instance_id)

                    query = f"UPDATE datahub_instances SET {', '.join(update_fields)} WHERE id = ?"
                    conn.execute(query, values)

                    return self.get_instance(instance_id)
                return None
        except Exception as e:
            print(f"Error updating instance: {e}")
            return None

    def delete_instance(self, instance_id: str) -> bool:
        """Delete a DataHub instance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM datahub_instances WHERE id = ?", (instance_id,)
                )
                return True
        except Exception as e:
            print(f"Error deleting instance: {e}")
            return False

    def list_instances(self) -> List[DataHubInstance]:
        """List all DataHub instances"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM datahub_instances ORDER BY created_at")
            rows = cursor.fetchall()
            return [self._row_to_instance(row) for row in rows]

    def has_default_instance(self) -> bool:
        """Check if there's a default instance configured"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM datahub_instances WHERE is_default = 1 AND is_active = 1"
            )
            result = cursor.fetchone()
            return bool(result[0] > 0) if result else False

    def get_default_instance(self) -> Optional[DataHubInstance]:
        """Get default DataHub instance"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM datahub_instances 
                WHERE is_default = 1 AND is_active = 1 
                LIMIT 1
            """
            )
            row = cursor.fetchone()
            return self._row_to_instance(row) if row else None

    def create_unknown_tenant_alert(
        self, tenant_id: str, team_id: Optional[str] = None
    ) -> UnknownTenant:
        """Create an unknown tenant alert"""
        with sqlite3.connect(self.db_path) as conn:
            # Try to update existing record
            cursor = conn.execute(
                """
                UPDATE unknown_tenants 
                SET last_seen = CURRENT_TIMESTAMP, event_count = event_count + 1
                WHERE tenant_id = ? AND team_id = ?
            """,
                (tenant_id, team_id),
            )

            # Insert if no existing record
            if cursor.rowcount == 0:
                unknown_tenant = UnknownTenant(
                    id=str(uuid.uuid4()),
                    tenant_id=tenant_id,
                    team_id=team_id,
                    first_seen=datetime.now(timezone.utc),
                    last_seen=datetime.now(timezone.utc),
                    event_count=1,
                )

                conn.execute(
                    """
                    INSERT INTO unknown_tenants (id, tenant_id, team_id, first_seen, last_seen, event_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        unknown_tenant.id,
                        unknown_tenant.tenant_id,
                        unknown_tenant.team_id,
                        unknown_tenant.first_seen,
                        unknown_tenant.last_seen,
                        unknown_tenant.event_count,
                    ),
                )

                return unknown_tenant
            else:
                # Return updated record
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM unknown_tenants WHERE tenant_id = ? AND team_id = ?",
                    (tenant_id, team_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError("Failed to find unknown tenant after update")
                return self._row_to_unknown_tenant(row)

    def get_unknown_tenants(self) -> List[UnknownTenant]:
        """Get all unknown tenants"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM unknown_tenants 
                ORDER BY event_count DESC, last_seen DESC
            """
            )
            rows = cursor.fetchall()
            return [self._row_to_unknown_tenant(row) for row in rows]

    def remove_unknown_tenant(self, tenant_id: str) -> bool:
        """Remove an unknown tenant"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM unknown_tenants WHERE tenant_id = ?", (tenant_id,)
                )
                return True
        except Exception as e:
            print(f"Error removing unknown tenant: {e}")
            return False

    def update_unknown_tenant(
        self, tenant_id: str, **kwargs
    ) -> Optional[UnknownTenant]:
        """Update an unknown tenant"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Build update query dynamically
                update_fields = []
                values = []
                for key, value in kwargs.items():
                    update_fields.append(f"{key} = ?")
                    values.append(value)

                if update_fields:
                    values.append(tenant_id)
                    query = f"UPDATE unknown_tenants SET {', '.join(update_fields)} WHERE tenant_id = ?"
                    conn.execute(query, values)

                    # Return updated record
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(
                        "SELECT * FROM unknown_tenants WHERE tenant_id = ?",
                        (tenant_id,),
                    )
                    row = cursor.fetchone()
                    return self._row_to_unknown_tenant(row) if row else None
                return None
        except Exception as e:
            print(f"Error updating unknown tenant: {e}")
            return None
