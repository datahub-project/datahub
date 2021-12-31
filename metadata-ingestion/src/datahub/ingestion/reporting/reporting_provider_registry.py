from datahub.ingestion.api.ingestion_job_reporting_provider_base import (
    IngestionReportingProviderBase,
)
from datahub.ingestion.api.registry import PluginRegistry

reporting_provider_registry = PluginRegistry[IngestionReportingProviderBase]()
reporting_provider_registry.register_from_entrypoint(
    "datahub.ingestion.reporting_provider.plugins"
)

# These providers are always enabled
assert reporting_provider_registry.get("datahub")
