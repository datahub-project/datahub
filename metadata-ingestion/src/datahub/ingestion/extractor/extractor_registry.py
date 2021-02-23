from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Extractor

from .mce_extractor import WorkUnitMCEExtractor

extractor_registry = Registry[Extractor]()

# Add a defaults to extractor registry.
extractor_registry.register("mce", WorkUnitMCEExtractor)
