from datahub.ingestion.api.registry import PluginRegistry
from datahub.lite.lite_local import DataHubLiteLocal

lite_registry = PluginRegistry[DataHubLiteLocal]()

# SQLite is the default because it ships with Python. DuckDB needs the
# `acryl-datahub[duckdb]` extra, and the registry surfaces that itself when the
# import fails.
lite_registry.register_lazy("sqlite", "datahub.lite.sqlite_lite:SqliteLite")
lite_registry.register_lazy("duckdb", "datahub.lite.duckdb_lite:DuckDBLite")
