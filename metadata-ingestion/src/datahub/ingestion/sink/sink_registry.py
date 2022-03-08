from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.sink import Sink

sink_registry = PluginRegistry[Sink]()
sink_registry.register_from_entrypoint("datahub.ingestion.sink.plugins")

# These sinks are always enabled
assert sink_registry.get("console")
assert sink_registry.get("file")
