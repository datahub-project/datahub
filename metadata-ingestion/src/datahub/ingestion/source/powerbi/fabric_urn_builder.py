"""Temporary URN builder for Fabric OneLake tables.

This module provides URN generation for DirectLake lineage.
It uses the centralized URN generation logic from datahub.ingestion.source.fabric.common.urn_generator.
"""

from typing import Optional

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.fabric.common.urn_generator import make_table_name
from datahub.ingestion.source.fabric.onelake.constants import (
    DEFAULT_SCHEMA_SCHEMALESS_LAKEHOUSE,
)

# Platform name for Fabric OneLake (must match PR #15888)
FABRIC_ONELAKE_PLATFORM = "fabric-onelake"


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
    # Use default schema for schemas-disabled lakehouses
    normalized_schema = (
        schema_name if schema_name else DEFAULT_SCHEMA_SCHEMALESS_LAKEHOUSE
    )
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
