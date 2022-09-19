import warnings

from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Source

source_registry = PluginRegistry[Source]()
source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")

# This source is always enabled
assert source_registry.get("file")

# Deprecations.
source_registry.register_alias(
    "snowflake-beta",
    "snowflake",
    lambda: warnings.warn(
        UserWarning("source type snowflake-beta is deprecated, use snowflake instead")
    ),
)
