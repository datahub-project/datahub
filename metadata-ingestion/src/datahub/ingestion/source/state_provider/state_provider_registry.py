from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import (
    IngestionCheckpointingProviderBase,
)
from datahub.ingestion.api.registry import PluginRegistry

ingestion_checkpoint_provider_registry = PluginRegistry[
    IngestionCheckpointingProviderBase
]()
ingestion_checkpoint_provider_registry.register_from_entrypoint(
    "datahub.ingestion.checkpointing_provider.plugins"
)
