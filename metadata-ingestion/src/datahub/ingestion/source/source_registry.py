from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Source

source_registry = Registry[Source]()
source_registry.load("datahub.ingestion.source", Source)
