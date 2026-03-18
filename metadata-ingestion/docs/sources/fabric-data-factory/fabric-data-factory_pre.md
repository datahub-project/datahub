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

- Workspaces as containers, data pipelines as DataFlows, activities as DataJobs
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

- **Fabric API permissions** (granted via Microsoft Entra ID): `Workspace.Read.All`, `Item.Read.All`, `Connection.Read.All`
- **Workspace access**: The identity must be added as a workspace member (Viewer or above) for each workspace you want to ingest

For execution history, the identity also needs:

- **Pipeline run access**: `Item.Execute.All` or equivalent permission to query pipeline and activity runs

#### Fabric Admin Settings

:::warning
For **service principal** and **managed identity** authentication, a Fabric administrator must enable API access for service principals in the Fabric admin portal. Without this, API calls will fail with 401/403 errors even if permissions are correctly assigned.
:::

1. Go to the [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal) > **Tenant settings**
2. Under **Developer settings**, enable **Service principals can use Fabric APIs**
3. Choose whether to allow all service principals or restrict to a specific security group

For detailed instructions, see [Enable service principal authentication](https://learn.microsoft.com/fabric/admin/metadata-scanning-enable-read-only-apis).

#### Authentication

The connector supports four authentication methods via the shared `credential` config block. All methods use Azure's `TokenCredential` interface.

##### Service Principal (recommended for production)

Register an application in [Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app) and note the `client_id`, `client_secret`, and `tenant_id`. Then grant the required Fabric API permissions and add the service principal as a member of the target workspaces.

```yaml
credential:
  authentication_method: service_principal
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
```

All three fields are required when using this method.

##### Managed Identity (for Azure-hosted deployments)

Use this when running DataHub ingestion on an Azure VM, AKS, App Service, or other Azure compute that supports [managed identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview). The managed identity must be added as a workspace member in Fabric.

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

Uses Azure's [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) chain, which tries multiple credential sources in order: environment variables, managed identity, Azure CLI, and more.

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
2. If using service principal or managed identity, ensure the Fabric admin has enabled API access for service principals (see **Fabric Admin Settings** above).
3. Grant the required Fabric API permissions to the identity and add it as a member of the target workspaces.
4. Configure the ingestion recipe with optional workspace and pipeline filters.
