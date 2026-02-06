# Microsoft Fabric OneLake Connector

This connector extracts metadata from Microsoft Fabric OneLake, including workspaces, lakehouses, warehouses, schemas, and tables.

## Quick Start

1. **Set up authentication** - Configure Azure credentials (see [Prerequisites](#prerequisites))
2. **Grant permissions** - Ensure your identity has `Workspace.Read.All` and workspace access
3. **Configure recipe** - Use `fabric-onelake_recipe.yml` as a template
4. **Run ingestion** - Execute `datahub ingest -c fabric-onelake_recipe.yml`

## Key Features

- Workspace, Lakehouse, Warehouse, and Schema containers
- Table datasets with proper subtypes
- Automatic detection and handling of schemas-enabled and schemas-disabled lakehouses
- Pattern-based filtering for workspaces, lakehouses, warehouses, and tables
- Stateful ingestion for stale entity removal
- Multiple authentication methods (Service Principal, Managed Identity, Azure CLI, DefaultAzureCredential)
