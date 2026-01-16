# Microsoft Fabric OneLake

This connector extracts metadata from Microsoft Fabric OneLake, including workspaces, lakehouses, warehouses, schemas, and tables.

## Concept Mapping

| Microsoft Fabric | DataHub Entity                            | Notes                                       |
| ---------------- | ----------------------------------------- | ------------------------------------------- |
| **Workspace**    | `Container` (subtype: `Fabric Workspace`) | Top-level organizational unit               |
| **Lakehouse**    | `Container` (subtype: `Fabric Lakehouse`) | Contains schemas and tables                 |
| **Warehouse**    | `Container` (subtype: `Fabric Warehouse`) | Contains schemas and tables                 |
| **Schema**       | `Container` (subtype: `Fabric Schema`)    | Logical grouping within lakehouse/warehouse |
| **Table**        | `Dataset`                                 | Tables within schemas                       |

### Hierarchy Structure

```
Platform (fabric-onelake)
└── Workspace (Container)
    ├── Lakehouse (Container)
    │   └── Schema (Container)
    │       └── Table (Dataset)
    └── Warehouse (Container)
        └── Schema (Container)
            └── Table/View (Dataset)
```

### Platform Instance as Tenant

The Fabric REST API does not expose tenant-level endpoints. To represent tenant-level organization in DataHub, set the `platform_instance` configuration field to your tenant identifier (e.g., "contoso-tenant"). This will be included in all container and dataset URNs, effectively grouping all workspaces under the specified platform instance/tenant.

## Prerequisites

### Authentication

The connector supports multiple Azure authentication methods:

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible environments                            | `authentication_method: default`                    |

For service principal setup, see [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

### Required Permissions

The connector requires **read-only access** to Fabric workspaces and their contents. The authenticated identity (service principal, managed identity, or user) must have:

**Workspace-Level Permissions:**

- **Workspace.Read.All** or **Workspace.ReadWrite.All** (Microsoft Entra delegated scope)
- **Viewer** role or higher in the Fabric workspaces you want to ingest

**API Permissions:**
The service principal or user must have the following Microsoft Entra API permissions:

- `Workspace.Read.All` (delegated) - Required to list and read workspace metadata
- Or `Workspace.ReadWrite.All` (delegated) - Provides read and write access

**Token Audiences:**
The connector uses two different token audiences depending on the operation:

- **Fabric REST API** (`https://api.fabric.microsoft.com`): Uses Power BI API scope (`https://analysis.windows.net/powerbi/api/.default`) for listing workspaces, lakehouses, warehouses, and basic table metadata
- **OneLake Delta Table APIs** (`https://onelake.table.fabric.microsoft.com`): Uses Storage audience (`https://storage.azure.com/.default`) for accessing schemas and tables in **schemas-enabled lakehouses**

The connector automatically handles both token audiences. For schemas-enabled lakehouses, it will use OneLake Delta Table APIs with Storage audience tokens. For schemas-disabled lakehouses, it uses the standard Fabric REST API.

**OneLake Data Access Permissions:**
For schemas-enabled lakehouses, you may also need OneLake data access permissions:

- If OneLake security is enabled on your lakehouse, ensure your identity has **Read** or **ReadWrite** permissions on the lakehouse item
- These permissions are separate from workspace roles and are managed in the Fabric portal under the lakehouse's security settings

**Note:** The connector automatically detects whether a lakehouse has schemas enabled and uses the appropriate API endpoint and token audience. No additional configuration is required.

For detailed information on permissions, see:

- [Fabric REST API Permissions](https://learn.microsoft.com/en-us/rest/api/fabric/articles/api-permissions)
- [Workspace Roles and Permissions](https://learn.microsoft.com/en-us/fabric/admin/roles)
- [OneLake Data Access Control](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model)

### Granting Permissions

**For Service Principal:**

1. Register an application in Microsoft Entra ID (Azure AD)
2. Grant API permissions:
   - Navigate to **Azure Portal** > **App registrations** > Your app > **API permissions**
   - Add permission: **Power BI Service** > **Delegated permissions** > `Workspace.Read.All`
   - Click **Grant admin consent** (if you have admin rights)
3. Assign workspace roles:
   - In Fabric portal, navigate to each workspace
   - Go to **Workspace settings** > **Access**
   - Add your service principal and assign **Viewer** role or higher

**For Managed Identity:**

1. Enable system-assigned managed identity on your Azure resource (VM, AKS, App Service, etc.)
2. Assign the managed identity to Fabric workspaces with **Viewer** role or higher
3. The connector will automatically use the managed identity for authentication

## Configuration

### Basic Recipe

```yaml
source:
  type: fabric-onelake
  config:
    # Authentication (using service principal)
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Optional: Platform instance (use as tenant identifier)
    # platform_instance: "contoso-tenant"

    # Optional: Environment
    # env: PROD

    # Optional: Filter workspaces by name pattern
    # workspace_pattern:
    #   allow:
    #     - "prod-.*"
    #   deny:
    #     - ".*-test"

    # Optional: Filter lakehouses by name pattern
    # lakehouse_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

    # Optional: Filter warehouses by name pattern
    # warehouse_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

    # Optional: Filter tables by name pattern
    # table_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Advanced Configuration

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Platform instance (represents tenant)
    platform_instance: "contoso-tenant"

    # Environment
    env: PROD

    # Filtering
    workspace_pattern:
      allow:
        - "prod-.*"
        - "shared-.*"
      deny:
        - ".*-test"
        - ".*-dev"

    lakehouse_pattern:
      allow:
        - ".*"
      deny:
        - ".*-backup"

    warehouse_pattern:
      allow:
        - ".*"
      deny: []

    table_pattern:
      allow:
        - ".*"
      deny:
        - ".*_temp"
        - ".*_backup"

    # Feature flags
    extract_lakehouses: true
    extract_warehouses: true
    extract_schemas: true # Set to false to skip schema containers

    # API timeout (seconds)
    api_timeout: 30

    # Stateful ingestion (optional)
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Using Managed Identity

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: managed_identity
      # For user-assigned managed identity, specify client_id
      # client_id: ${MANAGED_IDENTITY_CLIENT_ID}

    platform_instance: "contoso-tenant"
    env: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Using Azure CLI (Local Development)

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: cli
      # Run 'az login' first

    platform_instance: "contoso-tenant"
    env: DEV

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Schema Extraction

**Note:** Schema extraction (column metadata) is currently not implemented. The connector will ingest tables without column schemas. Schema extraction via SQL Analytics endpoint is planned for a future release.

## Schemas-Enabled vs Schemas-Disabled Lakehouses

The connector automatically handles both schemas-enabled and schemas-disabled lakehouses:

- **Schemas-Enabled Lakehouses**: The connector uses OneLake Delta Table APIs to list schemas first, then tables within each schema. This requires Storage audience tokens (`https://storage.azure.com/.default`).
- **Schemas-Disabled Lakehouses**: The connector uses the standard Fabric REST API `/tables` endpoint, which lists all tables under the default `dbo` schema. This uses Power BI API scope tokens.

The connector automatically detects the lakehouse type and uses the appropriate API endpoint. No configuration changes are needed.

## Stateful Ingestion

The connector supports stateful ingestion to track ingested entities and remove stale metadata. Enable it with:

```yaml
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

When enabled, the connector will:

- Track all ingested workspaces, lakehouses, warehouses, schemas, and tables
- Remove entities from DataHub that no longer exist in Fabric
- Maintain state across ingestion runs

## References

### Azure Authentication

- [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [Azure Identity Library](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [Service Principal Authentication](https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals)
- [Managed Identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview)

### Fabric Concepts

- [Microsoft Fabric Overview](https://learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview)
- [OneLake Overview](https://learn.microsoft.com/en-us/fabric/data-engineering/onelake-overview)
- [Workspaces in Fabric](https://learn.microsoft.com/en-us/fabric/get-started/workspaces)
- [Lakehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Warehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-overview)
