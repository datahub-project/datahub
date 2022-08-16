from datahub.ingestion.api.ingestion_run_summary_reporter import (
    IngestionRunSummaryReporter,
)
from datahub.ingestion.api.registry import PluginRegistry

reporting_provider_registry = PluginRegistry[IngestionRunSummaryReporter]()
reporting_provider_registry.register_from_entrypoint(
    "datahub.ingestion.reporting_provider.plugins"
)

# These providers are always enabled
assert reporting_provider_registry.get("datahub")
