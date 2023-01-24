from datahub.ingestion.api.registry import PluginRegistry
from datahub.lite.duckdb_lite import DuckDBLite
from datahub.lite.lite_local import DataHubLiteLocal

lite_registry = PluginRegistry[DataHubLiteLocal]()

# Add a defaults to lite registry.
lite_registry.register("duckdb", DuckDBLite)
