# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.api.registry import PluginRegistry
from datahub.lite.lite_local import DataHubLiteLocal

lite_registry = PluginRegistry[DataHubLiteLocal]()

# We currently only have one implementation.
lite_registry.register_lazy("duckdb", "datahub.lite.duckdb_lite:DuckDBLite")
