### Overview

The `fabric-onelake` module ingests metadata from Fabric Onelake into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

:::tip Quick Start

1. **Set up authentication** - Configure Azure credentials (see [Prerequisites](#prerequisites))
2. **Grant permissions** - Ensure your identity has `Workspace.Read.All` and workspace access
3. **Configure recipe** - Use `fabric-onelake_recipe.yml` as a template
4. **Run ingestion** - Execute `datahub ingest -c fabric-onelake_recipe.yml`
   :::

#### Key Features

- Workspace, Lakehouse, Warehouse, and Schema containers
- Table datasets with proper subtypes
- Automatic detection and handling of schemas-enabled and schemas-disabled lakehouses
- Pattern-based filtering for workspaces, lakehouses, warehouses, and tables
- Stateful ingestion for stale entity removal
- Multiple authentication methods (Service Principal, Managed Identity, Azure CLI, DefaultAzureCredential)

#### References

Azure Authentication

- [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [Azure Identity Library](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [Service Principal Authentication](https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals)
- [Managed Identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview)

Fabric Concepts

- [Microsoft Fabric Overview](https://learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview)
- [OneLake Overview](https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview)
- [Workspaces in Fabric](https://learn.microsoft.com/en-us/fabric/get-started/workspaces)
- [Lakehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Warehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Authentication

The connector supports multiple Azure authentication methods:

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible environments                            | `authentication_method: default`                    |

For service principal setup, see [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

#### Required Permissions

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

- [Fabric REST API Permissions](https://learn.microsoft.com/en-us/rest/api/fabric/articles/scopes)
- [Workspace Roles and Permissions](https://learn.microsoft.com/en-us/fabric/admin/roles)
- [OneLake Data Access Control](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model)

#### Granting Permissions

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
