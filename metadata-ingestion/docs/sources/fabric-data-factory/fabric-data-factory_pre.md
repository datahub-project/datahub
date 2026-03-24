### Overview

The `fabric-data-factory` module ingests metadata from Microsoft Fabric Data Factory into DataHub. It extracts workspaces, data pipelines, activities, and execution history, and resolves lineage from Copy activities to external datasets.

:::tip Quick Start

1. **Set up authentication** — Configure Azure credentials (see [Prerequisites](#prerequisites))
2. **Enable API access** — Ensure a Fabric admin has enabled service principal API access (if using SP or managed identity)
3. **Grant permissions** — Add your identity as a workspace member with Viewer role or higher
4. **Configure recipe** — Use `fabric-data-factory_recipe.yml` as a template
5. **Run ingestion** — Execute `datahub ingest -c fabric-data-factory_recipe.yml`
   :::

#### Key Features

- Workspaces as containers, data pipelines as DataFlows (DataHub entity type), activities as DataJobs
- Dataset-level lineage from Copy and InvokePipeline activities
- Pipeline and activity execution history as DataProcessInstances
- Cross-recipe lineage via `platform_instance_map` for connecting to externally ingested datasets
- Pattern-based filtering for workspaces and pipelines
- Stateful ingestion for stale entity removal
- Multiple authentication methods (Service Principal, Managed Identity, Azure CLI, DefaultAzureCredential)

#### References

Azure Authentication

- [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [Azure Identity Library](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [Service Principal Authentication](https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals)
- [Managed Identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview)

Fabric Data Factory Concepts

- [Microsoft Fabric Data Factory Overview](https://learn.microsoft.com/fabric/data-factory/data-factory-overview)
- [Data Pipelines in Fabric](https://learn.microsoft.com/fabric/data-factory/activity-overview)
- [Fabric REST API](https://learn.microsoft.com/rest/api/fabric/articles/)
- [Fabric Connections](https://learn.microsoft.com/fabric/data-factory/connector-overview)

### Prerequisites

#### Required Permissions

The connector requires read access to Fabric workspaces, items, and connections. The specific permissions depend on your authentication method, but the identity you use must have:

- **Workspace access**: The identity must be added as a workspace member (Viewer or above) for each workspace you want to ingest

##### Delegated (on behalf of a user) authentication

If using delegated auth (e.g., Azure CLI), request the following Fabric API scopes via Microsoft Entra ID:

- `Workspace.Read.All` — to list and read workspaces
- `Item.Read.All` — to list and read items (pipelines, activities)
- `Connection.Read.All` — to list and read connections for lineage resolution
  For execution history, additionally request:

- `Item.Execute.All` — to query pipeline and activity runs

##### Service Principal and Managed Identity authentication

Service principals and managed identities do not use delegated scopes. Instead, you need to:

1. **Enable API access**: A Fabric admin must enable the service principal tenant settings (see **Fabric Admin Settings** below)
2. **Grant workspace access**: Add the SP or MI as a workspace member (Viewer or above) for each workspace you want to ingest
3. **Grant connection access**: The SP or MI must have permission on the Fabric connections used by pipelines, so that the connector can read connection details for lineage resolution

#### Fabric Admin Settings

:::warning
For **service principal** and **managed identity** authentication, a Fabric administrator must enable API access for service principals in the Fabric admin portal. Without this, API calls will fail with 401/403 errors even if permissions are correctly assigned.
:::

As of mid-2025, Microsoft split the original single tenant setting into two separate settings. Configure them as follows:

1. Go to the [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal) > **Tenant settings**
2. Under **Developer settings**, enable the applicable setting(s):
   - **Service principals can call Fabric public APIs** — Controls access to CRUD APIs protected by the Fabric permission model (e.g., reading workspaces and items). This is **enabled by default** for new tenants since August 2025.
   - **Service principals can create workspaces, connections, and deployment pipelines** — Controls access to global APIs not protected by Fabric permissions. This is **disabled by default**. Enable only if needed.
3. Choose whether to enable for the entire organization or restrict to a specific security group. It is recommended to restrict access to a dedicated security group containing only the service principals that need API access.

> **Note**: If you are on an older tenant where the legacy single setting **Service principals can use Fabric APIs** is still visible, enable that instead. It will be automatically migrated to the two new settings.

For detailed instructions, see [Developer admin settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-developer) and [Identity support for Fabric REST APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support).

#### Authentication

The connector supports four authentication methods via the shared `credential` config block. All methods use Azure's `TokenCredential` interface.

##### Service Principal (recommended for production)

Register an application in [Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app) and note the `client_id`, `client_secret`, and `tenant_id`. Then ensure the Fabric admin has enabled service principal API access (see **Fabric Admin Settings** above) and add the service principal as a member of the target workspaces.

```yaml
credential:
  authentication_method: service_principal
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
```

All three fields are required when using this method.

##### Managed Identity (for Azure-hosted deployments)

Use this when running DataHub ingestion on an Azure VM, AKS, App Service, or other Azure compute that supports [managed identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview). The managed identity must be added as a workspace member in Fabric. A Fabric admin must also enable the tenant settings described in **Fabric Admin Settings** above — these settings govern API access for both service principals and managed identities, despite the setting name referencing only service principals.

```yaml
# System-assigned managed identity (no additional config needed)
credential:
  authentication_method: managed_identity
```

For **user-assigned managed identity**, provide the client ID:

```yaml
credential:
  authentication_method: managed_identity
  managed_identity_client_id: "<your-managed-identity-client-id>"
```

##### Azure CLI (for local development)

Uses the credentials from your local `az login` session. Run `az login` before starting ingestion. The signed-in user must have workspace access in Fabric.

```yaml
credential:
  authentication_method: cli
```

##### DefaultAzureCredential (flexible auto-detection)

Uses Azure's [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) chain, which tries multiple credential sources in order: environment variables, workload identity, managed identity, shared token cache, Azure CLI, Azure PowerShell, Azure Developer CLI, and more.

```yaml
credential:
  authentication_method: default
```

You can exclude specific credential sources from the chain to speed up detection or avoid unintended auth in mixed environments:

```yaml
credential:
  authentication_method: default
  exclude_cli_credential: true # Skip Azure CLI (recommended in production)
  exclude_environment_credential: false
  exclude_managed_identity_credential: false
```

#### Setup

1. Choose an authentication method from above and configure the `credential` block.
2. If using service principal or managed identity, ensure the Fabric admin has enabled the appropriate developer settings for service principal API access (see **Fabric Admin Settings** above).
3. Add the identity as a member of the target workspaces with Viewer role or above.
4. Configure the ingestion recipe with optional workspace and pipeline filters.
