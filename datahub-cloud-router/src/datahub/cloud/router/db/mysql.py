#!/usr/bin/env python3
"""
MySQL database implementation for DataHub Multi-Tenant Router.

This module provides a MySQL backend for the router's database operations,
supporting high-concurrency production deployments.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import pymysql
    from pymysql import Connection, cursors
except ImportError:
    print("❌ pymysql not available. Install with: pip install pymysql")
    raise

from .interface import Database
from .models import (
    DataHubInstance,
    DeploymentType,
    HealthStatus,
    TenantMapping,
    UnknownTenant,
)


class MySQLDatabase(Database):
    """MySQL implementation of the Database interface."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize MySQL database connection."""
        self.config = config

        # Extract configuration
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 3306)
        self.database = config.get("database", "datahub_router")
        self.username = config.get("username", "root")
        self.password = config.get("password", "")
        self.charset = config.get("charset", "utf8mb4")
        self.max_connections = config.get("max_connections", 10)

    def _get_connection(self) -> Connection:
        """Get a database connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
            charset=self.charset,
            autocommit=True,
            cursorclass=cursors.DictCursor,
        )

    def init_database(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Create tables
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS datahub_instances (
                        id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        deployment_type ENUM('cloud', 'on_premise', 'hybrid') NOT NULL,
                        url TEXT,
                        connection_id VARCHAR(255),
                        api_token TEXT,
                        health_check_url TEXT,
                        is_active BOOLEAN DEFAULT TRUE,
                        is_default BOOLEAN DEFAULT FALSE,
                        max_concurrent_requests INT DEFAULT 100,
                        timeout_seconds INT DEFAULT 30,
                        health_status ENUM('unknown', 'healthy', 'unhealthy') DEFAULT 'unknown',
                        created_at DATETIME NOT NULL,
                        updated_at DATETIME NOT NULL,
                        last_health_check DATETIME
                    )
                """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tenant_instance_mappings (
                        id VARCHAR(255) PRIMARY KEY,
                        tenant_id VARCHAR(255) NOT NULL,
                        datahub_instance_id VARCHAR(255) NOT NULL,
                        team_id VARCHAR(255),
                        created_at DATETIME NOT NULL,
                        created_by VARCHAR(255) DEFAULT 'system',
                        is_default_for_tenant BOOLEAN DEFAULT TRUE,
                        is_active BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (datahub_instance_id) REFERENCES datahub_instances(id) ON DELETE CASCADE,
                        UNIQUE KEY unique_tenant_team (tenant_id, team_id)
                    )
                """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS unknown_tenants (
                        id VARCHAR(255) PRIMARY KEY,
                        tenant_id VARCHAR(255) NOT NULL,
                        team_id VARCHAR(255),
                        team_name VARCHAR(255),
                        first_seen DATETIME NOT NULL,
                        last_seen DATETIME NOT NULL,
                        event_count INT DEFAULT 1,
                        status ENUM('new', 'assigned', 'resolved') DEFAULT 'new',
                        assigned_instance_id VARCHAR(255),
                        FOREIGN KEY (assigned_instance_id) REFERENCES datahub_instances(id) ON DELETE SET NULL
                    )
                """
                )

                conn.commit()
                print(f"✅ MySQL database schema initialized for {self.database}")

    def create_instance(
        self,
        name: str,
        url: str,
        deployment_type: DeploymentType = DeploymentType.CLOUD,
        **kwargs,
    ) -> DataHubInstance:
        """Create a new DataHub instance."""
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

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO datahub_instances (
                        id, name, deployment_type, url, connection_id, api_token,
                        health_check_url, is_active, is_default, max_concurrent_requests,
                        timeout_seconds, health_status, created_at, updated_at, last_health_check
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        current_time,
                        current_time,
                        instance.last_health_check,
                    ),
                )
                conn.commit()

        return instance

    def get_instance(self, instance_id: str) -> Optional[DataHubInstance]:
        """Get DataHub instance by ID."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM datahub_instances WHERE id = %s
                """,
                    (instance_id,),
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_instance(row)
                return None

    def list_instances(self) -> List[DataHubInstance]:
        """List all DataHub instances."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM datahub_instances ORDER BY created_at")
                rows = cursor.fetchall()
                return [self._row_to_instance(row) for row in rows]

    def has_default_instance(self) -> bool:
        """Check if there's a default instance."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) as count FROM datahub_instances WHERE is_default = TRUE
                """
                )
                result = cursor.fetchone()
                return bool(result["count"] > 0)

    def get_default_instance(self) -> Optional[DataHubInstance]:
        """Get the default DataHub instance."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM datahub_instances 
                    WHERE is_default = TRUE 
                    ORDER BY created_at 
                    LIMIT 1
                """
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_instance(row)
                return None

    def create_mapping(
        self,
        tenant_id: str,
        instance_id: str,
        team_id: Optional[str] = None,
        created_by: str = "system",
    ) -> TenantMapping:
        """Create a new tenant mapping."""
        current_time = datetime.now(timezone.utc)
        mapping = TenantMapping(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            instance_id=instance_id,
            team_id=team_id,
            created_by=created_by,
            created_at=current_time,
        )

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO tenant_instance_mappings (
                        id, tenant_id, datahub_instance_id, team_id, created_at, created_by,
                        is_default_for_tenant, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        mapping.id,
                        mapping.tenant_id,
                        mapping.instance_id,
                        mapping.team_id,
                        current_time,
                        mapping.created_by,
                        True,  # is_default_for_tenant
                        True,  # is_active
                    ),
                )
                conn.commit()

        return mapping

    def get_mapping(self, tenant_id: str) -> Optional[TenantMapping]:
        """Get tenant mapping by tenant ID."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM tenant_instance_mappings 
                    WHERE tenant_id = %s AND is_active = TRUE
                    ORDER BY is_default_for_tenant DESC, created_at
                    LIMIT 1
                """,
                    (tenant_id,),
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_mapping(row)
                return None

    def has_mapping(self, tenant_id: str) -> Optional[str]:
        """Check if tenant has a mapping and return instance ID."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT datahub_instance_id FROM tenant_instance_mappings 
                    WHERE tenant_id = %s AND is_active = TRUE
                    ORDER BY is_default_for_tenant DESC, created_at
                    LIMIT 1
                """,
                    (tenant_id,),
                )
                row = cursor.fetchone()

                if row:
                    return str(row["datahub_instance_id"])
                return None

    def create_unknown_tenant_alert(
        self, tenant_id: str, team_id: Optional[str] = None
    ) -> UnknownTenant:
        """Create or update an unknown tenant alert."""
        current_time = datetime.now(timezone.utc)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if unknown tenant already exists
                cursor.execute(
                    """
                    SELECT * FROM unknown_tenants 
                    WHERE tenant_id = %s AND team_id = %s
                """,
                    (tenant_id, team_id),
                )
                existing = cursor.fetchone()

                if existing:
                    # Update existing record
                    cursor.execute(
                        """
                        UPDATE unknown_tenants 
                        SET last_seen = %s, event_count = event_count + 1
                        WHERE id = %s
                    """,
                        (current_time, existing["id"]),
                    )

                    # Fetch updated record
                    cursor.execute(
                        """
                        SELECT * FROM unknown_tenants WHERE id = %s
                    """,
                        (existing["id"],),
                    )
                    row = cursor.fetchone()
                    conn.commit()
                    return self._row_to_unknown_tenant(row)
                else:
                    # Create new record
                    unknown = UnknownTenant(
                        id=str(uuid.uuid4()),
                        tenant_id=tenant_id,
                        team_id=team_id,
                        first_seen=current_time,
                        last_seen=current_time,
                        event_count=1,
                    )

                    cursor.execute(
                        """
                        INSERT INTO unknown_tenants (
                            id, tenant_id, team_id, first_seen, last_seen, event_count
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                        (
                            unknown.id,
                            unknown.tenant_id,
                            unknown.team_id,
                            current_time,
                            current_time,
                            unknown.event_count,
                        ),
                    )
                    conn.commit()
                    return unknown

    def get_unknown_tenants(self) -> List[UnknownTenant]:
        """Get all unknown tenants."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM unknown_tenants 
                    ORDER BY last_seen DESC
                """
                )
                rows = cursor.fetchall()
                return [self._row_to_unknown_tenant(row) for row in rows]

    def remove_unknown_tenant(self, tenant_id: str) -> bool:
        """Remove an unknown tenant alert."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM unknown_tenants WHERE tenant_id = %s
                """,
                    (tenant_id,),
                )
                conn.commit()
                return bool(cursor.rowcount > 0)

    def update_unknown_tenant(
        self, tenant_id: str, **kwargs
    ) -> Optional[UnknownTenant]:
        """Update an unknown tenant."""
        current_time = datetime.now(timezone.utc)

        # Build update query dynamically
        update_fields = []
        values = []

        for key, value in kwargs.items():
            if key in ["team_name", "status", "assigned_instance_id"]:
                update_fields.append(f"{key} = %s")
                values.append(value)

        if not update_fields:
            return None

        update_fields.append("last_seen = %s")
        values.append(current_time)
        values.append(tenant_id)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE unknown_tenants 
                    SET {", ".join(update_fields)}
                    WHERE tenant_id = %s
                """,
                    values,
                )
                conn.commit()

                if cursor.rowcount > 0:
                    # Return the updated unknown tenant
                    cursor.execute(
                        """
                        SELECT * FROM unknown_tenants WHERE tenant_id = %s
                    """,
                        (tenant_id,),
                    )
                    row = cursor.fetchone()
                    if row:
                        return self._row_to_unknown_tenant(row)
                return None

    def update_instance(self, instance_id: str, **kwargs) -> Optional[DataHubInstance]:
        """Update a DataHub instance."""
        current_time = datetime.now(timezone.utc)

        # Build update query dynamically
        update_fields = []
        values = []

        for key, value in kwargs.items():
            if key in ["deployment_type", "health_status"]:
                update_fields.append(f"{key} = %s")
                values.append(value.value if hasattr(value, "value") else value)
            else:
                update_fields.append(f"{key} = %s")
                values.append(value)

        if not update_fields:
            return self.get_instance(instance_id)

        update_fields.append("updated_at = %s")
        values.append(current_time)
        values.append(instance_id)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE datahub_instances 
                    SET {", ".join(update_fields)}
                    WHERE id = %s
                """,
                    values,
                )
                conn.commit()

                if cursor.rowcount > 0:
                    return self.get_instance(instance_id)
                return None

    def delete_instance(self, instance_id: str) -> bool:
        """Delete a DataHub instance."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM datahub_instances WHERE id = %s
                """,
                    (instance_id,),
                )
                conn.commit()
                return bool(cursor.rowcount > 0)

    def delete_mapping(self, tenant_id: str) -> bool:
        """Delete a tenant mapping."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM tenant_instance_mappings WHERE tenant_id = %s
                """,
                    (tenant_id,),
                )
                conn.commit()
                return bool(cursor.rowcount > 0)

    def _row_to_instance(self, row: Dict) -> DataHubInstance:
        """Convert database row to DataHubInstance."""
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
            created_at=row["created_at"] if row["created_at"] else None,
            updated_at=row["updated_at"] if row["updated_at"] else None,
            last_health_check=(
                row["last_health_check"] if row["last_health_check"] else None
            ),
        )

    def _row_to_mapping(self, row: Dict) -> TenantMapping:
        """Convert database row to TenantMapping."""
        return TenantMapping(
            id=row["id"],
            tenant_id=row["tenant_id"],
            instance_id=row["datahub_instance_id"],
            team_id=row["team_id"],
            created_by=row["created_by"],
            created_at=row["created_at"] if row["created_at"] else None,
        )

    def _row_to_unknown_tenant(self, row: Dict) -> UnknownTenant:
        """Convert database row to UnknownTenant."""
        return UnknownTenant(
            id=row["id"],
            tenant_id=row["tenant_id"],
            team_id=row["team_id"],
            first_seen=row["first_seen"].isoformat() if row["first_seen"] else None,
            last_seen=row["last_seen"].isoformat() if row["last_seen"] else None,
            event_count=row["event_count"],
        )

    def _cleanup_test_data(self):
        """Clean up test data by dropping all tables."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Drop tables in reverse order to handle foreign key constraints
                    cursor.execute("DROP TABLE IF EXISTS unknown_tenants")
                    cursor.execute("DROP TABLE IF EXISTS tenant_instance_mappings")
                    cursor.execute("DROP TABLE IF EXISTS datahub_instances")
                    conn.commit()
        except Exception as e:
            print(f"❌ Failed to cleanup test data: {e}")

    def close(self):
        """Close database connections."""
        # pymysql connections are automatically closed when using context managers
        pass
