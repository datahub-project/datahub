"""Temporary URN builder for Fabric OneLake tables.

This module provides URN generation for DirectLake lineage.
It mirrors the logic from datahub.ingestion.source.fabric.common.urn_generator
and should be removed once PR #15888 is merged, replacing imports with:
    from datahub.ingestion.source.fabric.common.urn_generator import make_table_name
"""

from typing import Optional

import datahub.emitter.mce_builder as builder

# URN pattern configuration (matching PR #15888)
# Pattern: {workspaceGUID}.{itemGUID}.{schema}.{table}
URN_PATTERN_SEPARATOR = "."

# Platform name for Fabric OneLake (must match PR #15888)
FABRIC_ONELAKE_PLATFORM = "fabric-onelake"


def make_table_name(
    workspace_id: str,
    item_id: str,
    schema_name: Optional[str],
    table_name: str,
) -> str:
    """Generate table name for dataset URN.

    This is the fully qualified name used in the dataset URN.

    Args:
        workspace_id: Workspace GUID
        item_id: Lakehouse or Warehouse GUID
        schema_name: Schema name (e.g., "dbo"). Use None or empty string for schemas-disabled lakehouses.
        table_name: Table name

    Returns:
        Table name string:
        - For schemas-enabled: {workspaceGUID}.{itemGUID}.{schema}.{table}
        - For schemas-disabled: {workspaceGUID}.{itemGUID}.{table} (schema is skipped)
    """
    if schema_name:
        return f"{workspace_id}{URN_PATTERN_SEPARATOR}{item_id}{URN_PATTERN_SEPARATOR}{schema_name}{URN_PATTERN_SEPARATOR}{table_name}"
    else:
        # Skip schema for schemas-disabled lakehouses
        return f"{workspace_id}{URN_PATTERN_SEPARATOR}{item_id}{URN_PATTERN_SEPARATOR}{table_name}"


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
        schema_name: Schema name (e.g., "dbo"). Defaults to None for schemas-disabled lakehouses.
        env: Environment (default: PROD)
        platform_instance: Optional platform instance

    Returns:
        Dataset URN for the OneLake table
    """
    qualified_name = make_table_name(
        workspace_id=workspace_id,
        item_id=item_id,
        schema_name=schema_name,
        table_name=table_name,
    )

    return builder.make_dataset_urn_with_platform_instance(
        platform=FABRIC_ONELAKE_PLATFORM,
        name=qualified_name,
        platform_instance=platform_instance,
        env=env,
    )
