"""Constants for Fabric OneLake ingestion."""

# Default T-SQL schema for the Fabric SQL layer. Applies to:
#   * schemas-disabled Lakehouses (catalog returns no schema_name → fall back to dbo)
#   * unqualified table refs in observed queries (parser default for dataset URN resolution)
# Fabric Warehouse and Lakehouse SQL Analytics endpoints both use dbo.
FABRIC_SQL_DEFAULT_SCHEMA = "dbo"

# SQL Server / Fabric schemas that are not user tables. Excluded from warehouse
# catalog listing and from SQL schema and view discovery.
# - INFORMATION_SCHEMA, sys: standard SQL Server system schemas.
# - queryinsights: Fabric Warehouse's Microsoft-managed Query Insights views.
FABRIC_SYSTEM_SCHEMAS: tuple[str, ...] = (
    "INFORMATION_SCHEMA",
    "sys",
    "queryinsights",
)
