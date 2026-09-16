"""Constants for Fabric OneLake ingestion."""

# Default T-SQL schema for the Fabric SQL layer. Applies to:
#   * schemas-disabled Lakehouses (catalog returns no schema_name → fall back to dbo)
#   * unqualified table refs in observed queries (parser default for dataset URN resolution)
# Fabric Warehouse and Lakehouse SQL Analytics endpoints both use dbo.
FABRIC_SQL_DEFAULT_SCHEMA = "dbo"
