from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.registry import PluginRegistry

reporting_provider_registry = PluginRegistry[PipelineRunListener]()
reporting_provider_registry.register_from_entrypoint(
    "datahub.ingestion.reporting_provider.plugins"
)

# These providers are always enabled
assert reporting_provider_registry.get("datahub")
assert reporting_provider_registry.get("file")
