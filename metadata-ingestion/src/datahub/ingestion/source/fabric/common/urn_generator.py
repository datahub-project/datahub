"""Centralized URN generation for Microsoft Fabric entities.

This module provides a single place for all URN generation logic,
making it easy to update the pattern in the future if needed.
"""

# URN pattern configuration
# Pattern: {workspaceGUID}.{itemGUID}.{schema}.{table}
# This can be easily changed in the future if needed
URN_PATTERN_SEPARATOR = "."


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
        schema_name: Schema name (e.g., "dbo")

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
        schema_name: Schema name (e.g., "dbo")
        table_name: Table name

    Returns:
        Table name string: {workspaceGUID}.{itemGUID}.{schema}.{table}
    """
    return f"{workspace_id}{URN_PATTERN_SEPARATOR}{item_id}{URN_PATTERN_SEPARATOR}{schema_name}{URN_PATTERN_SEPARATOR}{table_name}"
