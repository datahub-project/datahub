"""Centralized URN generation for Microsoft Fabric entities.

This module provides a single place for all URN generation logic,
making it easy to update the pattern in the future if needed.
"""

from typing import Optional

import datahub.emitter.mce_builder as builder

# URN pattern configuration
# Pattern: {workspaceGUID}.{itemGUID}.{schema}.{table}
# This can be easily changed in the future if needed
URN_PATTERN_SEPARATOR = "."

# Platform name for Fabric OneLake (must match OneLake connector)
FABRIC_ONELAKE_PLATFORM = "fabric-onelake"

# Default schema for schemas-disabled Fabric Lakehouses
DEFAULT_SCHEMA_FOR_SCHEMALESS = "dbo"


def make_workspace_name(workspace_id: str) -> str:
    """Generate workspace name for container URN.

    Args:
        workspace_id: Workspace GUID

    Returns:
        Workspace name string
    """
    return workspace_id


def make_lakehouse_name(workspace_id: str, lakehouse_id: str) -> str:
    """Generate lakehouse name for container URN.

    Args:
        workspace_id: Workspace GUID
        lakehouse_id: Lakehouse GUID

    Returns:
        Lakehouse name string: {workspaceGUID}.{lakehouseGUID}
    """
    return f"{workspace_id}{URN_PATTERN_SEPARATOR}{lakehouse_id}"


def make_warehouse_name(workspace_id: str, warehouse_id: str) -> str:
    """Generate warehouse name for container URN.

    Args:
        workspace_id: Workspace GUID
        warehouse_id: Warehouse GUID

    Returns:
        Warehouse name string: {workspaceGUID}.{warehouseGUID}
    """
    return f"{workspace_id}{URN_PATTERN_SEPARATOR}{warehouse_id}"


def make_schema_name(workspace_id: str, item_id: str, schema_name: str) -> str:
    """Generate schema name for container URN.

    Args:
        workspace_id: Workspace GUID
        item_id: Lakehouse or Warehouse GUID
        schema_name: Schema name

    Returns:
        Schema name string: {workspaceGUID}.{itemGUID}.{schema}
    """
    return f"{workspace_id}{URN_PATTERN_SEPARATOR}{item_id}{URN_PATTERN_SEPARATOR}{schema_name}"


def make_table_name(
    workspace_id: str, item_id: str, schema_name: str, table_name: str
) -> str:
    """Generate table name for dataset URN.

    This is the full qualified name used in the dataset URN.

    Args:
        workspace_id: Workspace GUID
        item_id: Lakehouse or Warehouse GUID
        schema_name: Schema name
        table_name: Table name

    Returns:
        Table name string: {workspaceGUID}.{itemGUID}.{schema}.{table}
    """
    return f"{workspace_id}{URN_PATTERN_SEPARATOR}{item_id}{URN_PATTERN_SEPARATOR}{schema_name}{URN_PATTERN_SEPARATOR}{table_name}"


def make_onelake_urn(
    workspace_id: str,
    item_id: str,
    table_name: str,
    schema_name: Optional[str] = None,
    env: str = "PROD",
    platform_instance: Optional[str] = None,
) -> str:
    """Create a URN for an OneLake table (Lakehouse/Warehouse table in Fabric).

    Args:
        workspace_id: Workspace GUID
        item_id: Lakehouse or Warehouse GUID
        table_name: Table name
        schema_name: Schema name (e.g., "dbo"). Defaults to "dbo" for schemas-disabled lakehouses.
        env: Environment (default: PROD)
        platform_instance: Optional platform instance

    Returns:
        Dataset URN for the OneLake table
    """
    normalized_schema = schema_name if schema_name else DEFAULT_SCHEMA_FOR_SCHEMALESS
    qualified_name = make_table_name(
        workspace_id=workspace_id,
        item_id=item_id,
        schema_name=normalized_schema,
        table_name=table_name,
    )
    return builder.make_dataset_urn_with_platform_instance(
        platform=FABRIC_ONELAKE_PLATFORM,
        name=qualified_name,
        platform_instance=platform_instance,
        env=env,
    )
