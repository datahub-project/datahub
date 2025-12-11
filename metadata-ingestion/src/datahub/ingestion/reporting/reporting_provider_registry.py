# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.registry import PluginRegistry

reporting_provider_registry = PluginRegistry[PipelineRunListener]()
reporting_provider_registry.register_from_entrypoint(
    "datahub.ingestion.reporting_provider.plugins"
)
