"""
Backward compatibility shim for SnowflakeTagPropagatorAction.

This module provides backward compatibility for existing automations that reference
the old class path 'datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction'.

The old SnowflakeTagPropagatorAction class has been renamed to SnowflakeMetadataSyncAction
and moved to metadata_sync_action.py, with additional support for description synchronization.

This shim ensures that existing automations continue to work without modification.
"""

# Import the new implementation
from datahub_integrations.propagation.snowflake.metadata_sync_action import (
    SnowflakeMetadataSyncAction,
    SnowflakeMetadataSyncConfig,
)

# Re-export with the old names for backward compatibility
SnowflakeTagPropagatorAction = SnowflakeMetadataSyncAction
SnowflakeTagPropagatorConfig = SnowflakeMetadataSyncConfig

__all__ = [
    "SnowflakeTagPropagatorAction",
    "SnowflakeTagPropagatorConfig",
]
