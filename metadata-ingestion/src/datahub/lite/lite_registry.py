from datahub.ingestion.api.registry import PluginRegistry
from datahub.lite.lite_local import DataHubLiteLocal

lite_registry = PluginRegistry[DataHubLiteLocal]()

# We currently only have one implementation.
lite_registry.register_lazy("duckdb", "datahub.lite.duckdb_lite:DuckDBLite")
