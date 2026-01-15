# Microsoft Fabric OneLake Connector

This connector extracts metadata from Microsoft Fabric OneLake, including workspaces, lakehouses, warehouses, schemas, and tables.

## Quick Start

1. **Set up authentication** - See [Prerequisites](#prerequisites) section
2. **Configure permissions** - Grant `Workspace.Read.All` to your service principal
3. **Use the recipe** - See `fabric_onelake_recipe.yml` for a starter template
4. **Run ingestion** - Execute `datahub ingest -c fabric_onelake_recipe.yml`

## Documentation

- **[fabric_onelake.md](./fabric_onelake.md)** - Full documentation including:

  - Concept mapping
  - Prerequisites and permissions
  - Configuration options
  - Recipe examples
  - Links to Azure/Fabric documentation

- **[fabric_onelake_recipe.yml](./fabric_onelake_recipe.yml)** - Example recipe template

## Key Features

- ✅ Workspace, Lakehouse, Warehouse, and Schema containers
- ✅ Table and View datasets
- ✅ Hierarchical organization
- ✅ Pattern-based filtering
- ✅ Stateful ingestion support
- ✅ Multiple authentication methods (Service Principal, Managed Identity, Azure CLI, DefaultAzureCredential)

## Prerequisites

- Microsoft Entra ID (Azure AD) application with `Workspace.Read.All` permission
- Viewer role or higher in Fabric workspaces you want to ingest
- See [fabric_onelake.md](./fabric_onelake.md) for detailed setup instructions

## Related Documentation

- [Microsoft Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/)
- [Fabric Workspaces](https://learn.microsoft.com/en-us/fabric/get-started/workspaces)
- [OneLake Overview](https://learn.microsoft.com/en-us/fabric/data-engineering/onelake-overview)
