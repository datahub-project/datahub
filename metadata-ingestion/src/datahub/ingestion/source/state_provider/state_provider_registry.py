# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
