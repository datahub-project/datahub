"""
Test utilities for Hive Metastore connector tests.

This module provides helper functions and fixtures for testing the Hive
Metastore connector without requiring a full configuration setup.
"""

from typing import Optional
from unittest.mock import MagicMock

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.hive.hive_thrift_client import (
    ThriftConnectionConfig,
)
from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import ThriftDataFetcher


def create_thrift_fetcher_for_test(
    host: str,
    port: int = 9083,
    use_kerberos: bool = False,
    kerberos_service_name: str = "hive",
    kerberos_hostname_override: Optional[str] = None,
    timeout_seconds: int = 60,
    database_pattern: Optional[AllowDenyPattern] = None,
    catalog_name: Optional[str] = None,
    max_retries: int = 1,
) -> ThriftDataFetcher:
    """
    Factory function for creating a ThriftDataFetcher for testing.

    This provides a simpler interface than constructing a full HiveMetastore config.

    Args:
        host: HMS host
        port: HMS Thrift port (default 9083)
        use_kerberos: Whether to use Kerberos authentication
        kerberos_service_name: Kerberos service name (default "hive")
        kerberos_hostname_override: Optional hostname override for Kerberos
        timeout_seconds: Connection timeout
        database_pattern: Pattern for filtering databases
        catalog_name: HMS 3.x catalog name
        max_retries: Number of retries for transient failures

    Returns:
        Configured ThriftDataFetcher instance
    """
    # Create a minimal mock config with required attributes
    mock_config = MagicMock()
    mock_config.host_port = f"{host}:{port}"
    mock_config.use_kerberos = use_kerberos
    mock_config.kerberos_service_name = kerberos_service_name
    mock_config.kerberos_hostname_override = kerberos_hostname_override
    mock_config.timeout_seconds = timeout_seconds
    mock_config.database_pattern = database_pattern or AllowDenyPattern.allow_all()
    mock_config.catalog_name = catalog_name

    fetcher = ThriftDataFetcher(mock_config)
    # Override the thrift config with custom max_retries
    fetcher._thrift_config = ThriftConnectionConfig(
        host=host,
        port=port,
        use_kerberos=use_kerberos,
        kerberos_service_name=kerberos_service_name,
        kerberos_hostname_override=kerberos_hostname_override,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    return fetcher
