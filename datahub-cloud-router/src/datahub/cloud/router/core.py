"""
Core router logic for DataHub Cloud Router.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from .db import DatabaseConfig, DatabaseFactory
from .db.models import DataHubInstance, DeploymentType

logger = logging.getLogger(__name__)


class UnknownTenantError(Exception):
    """Exception raised when a tenant is not found in the routing configuration."""

    pass


class MultiTenantRouter:
    """Multi-tenant router for Teams webhook events to DataHub instances."""

    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        """Initialize the router with database configuration."""
        if db_config is None:
            db_config = DatabaseConfig(
                type=os.getenv("DATAHUB_ROUTER_DB_TYPE", "sqlite"),
                path=os.getenv("DATAHUB_ROUTER_DB_PATH", ".dev/router.db"),
            )

        self.db_factory = DatabaseFactory(db_config)
        self.db = self.db_factory.get_database()

        # Ensure default instance exists
        self._ensure_default_instance()

        # Get default target URL
        self.default_target_url = os.getenv(
            "DATAHUB_ROUTER_TARGET_URL", "http://localhost:9003"
        )

        print(
            f"🔄 Multi-tenant routing enabled -> Default target: {self.default_target_url}"
        )
        print(
            f"💾 Router database: {db_config.path if db_config.path else 'in-memory'}"
        )

    def _ensure_default_instance(self) -> None:
        """Ensure a default DataHub instance exists."""
        if not self.db.has_default_instance():
            print("✅ Created default local instance pointing to http://localhost:9003")
            self.db.create_instance(
                name="Default Local DataHub",
                url="http://localhost:9003",
                deployment_type=DeploymentType.CLOUD,
                is_default=True,
                health_check_url="http://localhost:9003/health",
            )

    def _get_instance_by_tenant_and_team(
        self, tenant_id: str, team_id: str
    ) -> Optional[DataHubInstance]:
        """Get DataHub instance for specific tenant and team."""
        # First try to get specific team mapping
        instance_id = self.db.has_mapping(tenant_id)
        if instance_id:
            return self.db.get_instance(instance_id)

        # Fall back to default instance
        return self.db.get_default_instance()

    def _get_instance_by_tenant(self, tenant_id: str) -> Optional[DataHubInstance]:
        """Get DataHub instance for tenant (default mapping)."""
        instance_id = self.db.has_mapping(tenant_id)
        if instance_id:
            return self.db.get_instance(instance_id)

        # Fall back to default instance
        return self.db.get_default_instance()

    def _has_default_instance(self) -> bool:
        """Check if default instance exists."""
        return self.db.has_default_instance()

    def _get_default_instance(self) -> Optional[DataHubInstance]:
        """Get the default DataHub instance."""
        return self.db.get_default_instance()

    def _create_unknown_tenant_alert(
        self, tenant_id: str, team_id: Optional[str] = None
    ) -> None:
        """Create alert for unknown tenant."""
        self.db.create_unknown_tenant_alert(tenant_id, team_id)

    async def route_event(
        self,
        event_data: Dict[str, Any],
        tenant_id: str,
        team_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Route a Teams event to the appropriate DataHub instance."""
        start_time = datetime.now(timezone.utc)

        logger.info(f"Routing event for tenant_id={tenant_id}, team_id={team_id}")

        try:
            # Determine target instance
            target_instance = None
            if team_id:
                logger.debug(
                    f"Looking for team-specific mapping for tenant={tenant_id}, team={team_id}"
                )
                target_instance = self._get_instance_by_tenant_and_team(
                    tenant_id, team_id
                )
            else:
                logger.debug(f"Looking for tenant mapping for tenant={tenant_id}")
                target_instance = self._get_instance_by_tenant(tenant_id)

            if not target_instance:
                logger.warning(f"No target instance found for tenant {tenant_id}")
                # No mapping found, create unknown tenant alert
                self._create_unknown_tenant_alert(tenant_id, team_id)

                return {
                    "status": "error",
                    "message": f"No DataHub instance mapping found for tenant {tenant_id}",
                    "tenant_id": tenant_id,
                    "team_id": team_id,
                    "timestamp": start_time.isoformat(),
                }

            logger.info(
                f"Found target instance: {target_instance.name} ({target_instance.url})"
            )

            # Forward event to target instance
            target_url = f"{target_instance.url}/integrations/teams/webhook"
            logger.info(f"Forwarding event to: {target_url}")

            # Use provided headers or default ones
            forward_headers = (
                headers if headers else {"Content-Type": "application/json"}
            )
            logger.debug(f"Forwarding with headers: {list(forward_headers.keys())}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    target_url,
                    json=event_data,
                    headers=forward_headers,
                    timeout=aiohttp.ClientTimeout(
                        total=target_instance.timeout_seconds
                    ),
                ) as response:
                    response_data = await response.text()

                    end_time = datetime.now(timezone.utc)
                    processing_time = int(
                        (end_time - start_time).total_seconds() * 1000
                    )

                    if response.status == 200:
                        logger.info(
                            f"Successfully forwarded event to {target_instance.name} in {processing_time}ms"
                        )
                        return {
                            "status": "success",
                            "target_instance": target_instance.name,
                            "target_url": target_url,
                            "tenant_id": tenant_id,
                            "team_id": team_id,
                            "processing_time_ms": processing_time,
                            "timestamp": end_time.isoformat(),
                        }
                    else:
                        logger.error(
                            f"Error forwarding event to {target_instance.name}: {response.status} - {response_data[:200]}..."
                        )
                        return {
                            "status": "error",
                            "target_instance": target_instance.name,
                            "target_url": target_url,
                            "tenant_id": tenant_id,
                            "team_id": team_id,
                            "http_status": response.status,
                            "response_data": response_data,
                            "processing_time_ms": processing_time,
                            "timestamp": end_time.isoformat(),
                        }

        except asyncio.TimeoutError:
            end_time = datetime.now(timezone.utc)
            processing_time = int((end_time - start_time).total_seconds() * 1000)

            return {
                "status": "error",
                "message": "Request timeout",
                "tenant_id": tenant_id,
                "team_id": team_id,
                "processing_time_ms": processing_time,
                "timestamp": end_time.isoformat(),
            }

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            processing_time = int((end_time - start_time).total_seconds() * 1000)

            return {
                "status": "error",
                "message": str(e),
                "tenant_id": tenant_id,
                "team_id": team_id,
                "processing_time_ms": processing_time,
                "timestamp": end_time.isoformat(),
            }

    def get_routing_stats(self) -> Dict[str, Any]:
        """Get routing statistics."""
        instances = self.db.list_instances()
        unknown_tenants = self.db.get_unknown_tenants()

        return {
            "total_instances": len(instances),
            "active_instances": len([i for i in instances if i.is_active]),
            "default_instance": (
                default_instance.name
                if (default_instance := self._get_default_instance()) is not None
                else None
            ),
            "unknown_tenants_count": len(unknown_tenants),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
