from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Extractor
from datahub.ingestion.extractor.mce_extractor import WorkUnitRecordExtractor

extractor_registry = PluginRegistry[Extractor]()

# Add a defaults to extractor registry.
extractor_registry.register("generic", WorkUnitRecordExtractor)
