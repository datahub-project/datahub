## Overview

Microsoft Fabric OneLake is a storage and lakehouse platform. Learn more in the [official Microsoft Fabric OneLake documentation](https://learn.microsoft.com/fabric/onelake/onelake-overview).

The DataHub integration for Microsoft Fabric OneLake covers file/lakehouse metadata entities such as datasets, paths, and containers. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

The mapping below provides a platform-level view. Module-specific mappings and nuances are documented in each module section.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |

Modules on this platform: `fabric-onelake`.

### Microsoft Fabric OneLake Connector

This connector extracts metadata from Microsoft Fabric OneLake, including workspaces, lakehouses, warehouses, schemas, and tables.

### Quick Start

1. **Set up authentication** - Configure Azure credentials (see [Prerequisites](#prerequisites))
2. **Grant permissions** - Ensure your identity has `Workspace.Read.All` and workspace access
3. **Configure recipe** - Use `fabric-onelake_recipe.yml` as a template
4. **Run ingestion** - Execute `datahub ingest -c fabric-onelake_recipe.yml`

### Key Features

- Workspace, Lakehouse, Warehouse, and Schema containers
- Table datasets with proper subtypes
- Automatic detection and handling of schemas-enabled and schemas-disabled lakehouses
- Pattern-based filtering for workspaces, lakehouses, warehouses, and tables
- Stateful ingestion for stale entity removal
- Multiple authentication methods (Service Principal, Managed Identity, Azure CLI, DefaultAzureCredential)
