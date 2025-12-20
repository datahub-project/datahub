"""
Thrift-based data fetcher for Hive Metastore.

This module provides the ThriftDataFetcher class that implements the HiveDataFetcher
Protocol using the HMS Thrift API. It handles:
- Kerberos/SASL authentication
- HMS 3.x catalog support
- Database pattern filtering
- Connection management
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from datahub.ingestion.source.sql.hive.hive_thrift_client import (
    HiveMetastoreThriftClient,
    ThriftConnectionConfig,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_metastore_config import HiveMetastore

logger = logging.getLogger(__name__)


class ThriftDataFetcher:
    """
    Data fetcher using HMS Thrift API.

    This implementation connects to Hive Metastore via the Thrift protocol,
    supporting both plain and Kerberos-authenticated connections.

    Supports:
    - Kerberos/SASL authentication
    - HMS 3.x multi-catalog deployments
    - Pattern-based database filtering
    - Automatic retry with exponential backoff
    """

    def __init__(self, config: "HiveMetastore"):
        """
        Initialize the Thrift data fetcher.

        Args:
            config: HiveMetastore configuration with Thrift connection settings
        """
        self.config = config
        self._thrift_config = self._create_thrift_config(config)
        self._client: Optional[HiveMetastoreThriftClient] = None
        self._databases: Optional[List[str]] = None

    @staticmethod
    def _create_thrift_config(
        config: "HiveMetastore", max_retries: Optional[int] = None
    ) -> ThriftConnectionConfig:
        """Create ThriftConnectionConfig from source config."""
        host_port_parts = config.host_port.split(":")
        host = host_port_parts[0]
        port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 9083

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": port,
            "use_kerberos": config.use_kerberos,
            "kerberos_service_name": config.kerberos_service_name,
            "kerberos_hostname_override": config.kerberos_hostname_override,
            "timeout_seconds": config.timeout_seconds,
        }
        if max_retries is not None:
            kwargs["max_retries"] = max_retries

        return ThriftConnectionConfig(**kwargs)

    def _ensure_connected(self) -> HiveMetastoreThriftClient:
        """Ensure Thrift client is connected and return it."""
        if self._client is None:
            client = HiveMetastoreThriftClient(self._thrift_config)
            client.connect()
            self._client = client
        return self._client

    def _get_catalog_name(self) -> Optional[str]:
        """Get catalog name from config, or None for default catalog."""
        return self.config.catalog_name

    def _get_databases_to_process(self) -> List[str]:
        """Get list of databases, applying pattern filter."""
        if self._databases is None:
            client = self._ensure_connected()
            catalog_name = self._get_catalog_name()
            all_databases = client.get_all_databases(catalog_name)

            # Apply database_pattern filter
            self._databases = [
                db for db in all_databases if self.config.database_pattern.allowed(db)
            ]
            catalog_info = f" in catalog '{catalog_name}'" if catalog_name else ""
            logger.info(
                f"Found {len(all_databases)} databases{catalog_info}, "
                f"{len(self._databases)} after filtering"
            )

        return self._databases

    def fetch_table_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table/column rows via Thrift API."""
        client = self._ensure_connected()
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return client.iter_table_rows(databases, catalog_name)

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch Hive view rows via Thrift API."""
        client = self._ensure_connected()
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return client.iter_view_rows(databases, catalog_name)

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch schema/database rows via Thrift API."""
        client = self._ensure_connected()
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return client.iter_schema_rows(databases, catalog_name)

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        """Fetch table properties rows via Thrift API."""
        client = self._ensure_connected()
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return client.iter_table_properties_rows(databases, catalog_name)

    def get_database_failures(self) -> List[Tuple[str, str]]:
        """Get list of database failures for reporting."""
        if self._client is not None:
            return self._client.get_database_failures()
        return []

    def get_table_failures(self) -> List[Tuple[str, str, str]]:
        """Get list of table failures for reporting."""
        if self._client is not None:
            return self._client.get_table_failures()
        return []

    def close(self) -> None:
        """Close the Thrift connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
