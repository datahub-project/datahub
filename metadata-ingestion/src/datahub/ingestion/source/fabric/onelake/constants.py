"""Constants for Fabric OneLake ingestion."""

# Default T-SQL schema for the Fabric SQL layer. Applies to:
#   * schemas-disabled Lakehouses (catalog returns no schema_name → fall back to dbo)
#   * unqualified table refs in observed queries (parser default for dataset URN resolution)
# Fabric Warehouse and Lakehouse SQL Analytics endpoints both use dbo.
FABRIC_SQL_DEFAULT_SCHEMA = "dbo"

# Schemas of the Fabric SQL endpoints that hold system / Microsoft-managed
# objects rather than user metadata:
# - INFORMATION_SCHEMA, sys: standard SQL Server catalog schemas.
# - queryinsights: Fabric's Query Insights views (exec_requests_history, ...).
#   See https://learn.microsoft.com/fabric/data-warehouse/query-insights
# They are excluded from table / view discovery, and SQL references to them
# (e.g. from queries the ODBC driver's catalog procedures run) never produce
# datasets, lineage, usage, or operations.
FABRIC_SYSTEM_SCHEMAS: tuple[str, ...] = (
    "INFORMATION_SCHEMA",
    "sys",
    "queryinsights",
)
_FABRIC_SYSTEM_SCHEMAS_FOLDED = frozenset(s.casefold() for s in FABRIC_SYSTEM_SCHEMAS)


def is_fabric_system_schema(schema_name: str) -> bool:
    """Whether ``schema_name`` is a Fabric SQL system schema (case-insensitive)."""
    return schema_name.casefold() in _FABRIC_SYSTEM_SCHEMAS_FOLDED
