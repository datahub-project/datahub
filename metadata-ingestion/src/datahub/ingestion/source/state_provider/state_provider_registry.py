from datahub.ingestion.api.ingestion_state_provider import IngestionStateProvider
from datahub.ingestion.api.registry import PluginRegistry

ingestion_state_provider_registry = PluginRegistry[IngestionStateProvider]()
ingestion_state_provider_registry.register_from_entrypoint(
    "datahub.ingestion.state_provider.plugins"
)

# These sinks are always enabled
assert ingestion_state_provider_registry.get("datahub")
