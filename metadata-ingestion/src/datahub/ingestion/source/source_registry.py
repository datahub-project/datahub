from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Source

source_registry = Registry[Source]()
source_registry.load("datahub.ingestion.source.plugins")

# This source is always enabled
assert source_registry.get("file")
