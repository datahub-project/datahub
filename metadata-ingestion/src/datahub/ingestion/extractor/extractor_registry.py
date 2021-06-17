from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Extractor

from .mce_extractor import WorkUnitRecordExtractor

extractor_registry = Registry[Extractor]()

# Add a defaults to extractor registry.
extractor_registry.register("generic", WorkUnitRecordExtractor)
