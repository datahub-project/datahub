


# Fabric Data Factory

## Overview

Microsoft Fabric Data Factory is a cloud-based data integration service within the Microsoft Fabric platform. Learn more in the [official Microsoft Fabric Data Factory documentation](https://learn.microsoft.com/fabric/data-factory/data-factory-overview).

The DataHub integration for Fabric Data Factory covers pipeline and orchestration entities such as workspaces, data pipelines, and activities. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

| Fabric Data Factory Concept | DataHub Entity                            | Notes                                                         |
| --------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
| **Workspace**               | `Container` (subtype: `Fabric Workspace`) | Top-level organizational unit                                 |
| **Data Pipeline**           | `DataFlow`                                | Orchestration pipeline containing activities                  |
| **Activity**                | `DataJob`                                 | Individual task within a pipeline (Copy, Lookup, Spark, etc.) |
| **Pipeline Run**            | `DataProcessInstance`                     | Execution record for a pipeline run                           |
| **Activity Run**            | `DataProcessInstance`                     | Execution record for an individual activity within a pipeline |
| **Connection**              | _(resolved to external Dataset)_          | Used for lineage resolution to datasets on external platforms |

### Hierarchy Structure

```
Platform (fabric-data-factory)
└── Workspace (Container)
    └── Data Pipeline (DataFlow)
        └── Activity (DataJob)
            ├── Pipeline Run (DataProcessInstance)
            └── Activity Run (DataProcessInstance)
```


## Module `fabric-data-factory`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Optionally enabled via stateful_ingestion config. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default via Copy and InvokePipeline activities. |

### Overview

The `fabric-data-factory` module ingests metadata from Microsoft Fabric Data Factory into DataHub. It extracts workspaces, data pipelines, activities, and execution history, and resolves lineage from Copy activities to external datasets.

:::tip Quick Start

1. **Set up authentication** — Configure Azure credentials (see [Prerequisites](#prerequisites))
2. **Enable API access** — Ensure a Fabric admin has enabled service principal API access (if using SP or managed identity)
3. **Grant permissions** — Add your identity as a workspace **Contributor** (required for pipeline definitions and lineage)
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

The connector requires **Contributor** role on each workspace. Contributor is needed to fetch pipeline definitions without it. With Reader role only, the connector will list workspaces and pipelines but will not extract pipeline activities, activity run details, or lineage.

##### Delegated (on behalf of a user) authentication

If using delegated auth (e.g., Azure CLI), the signed-in user's existing Fabric permissions apply directly. The connector requires the following delegated scopes:

- `Workspace.Read.All` or `Workspace.ReadWrite.All` — for listing workspaces and items
- `Item.ReadWrite.All` or `DataPipeline.ReadWrite.All` — for Get Item Definition, List Item Connections, and Query Activity Runs (`Item.Read.All` is **not** sufficient for definitions and connections)
- `Item.Read.All` or `DataPipeline.Read.All` — sufficient for List Item Job Instances (execution history)

The Azure CLI token includes the necessary Fabric API scopes by default.

##### Service Principal and Managed Identity authentication

Service principals and managed identities do not inherit any permissions by default. You need to:

1. **Enable API access**: A Fabric admin must enable the service principal tenant settings (see **Fabric Admin Settings** below)
2. **Grant workspace access**: Add the SP or MI as a workspace **Contributor** for each workspace you want to ingest

#### Fabric Admin Settings

:::warning
For **service principal** and **managed identity** authentication, a Fabric administrator must enable API access for service principals in the Fabric admin portal. Without this, API calls will fail with 401 errors even if workspace permissions are correctly assigned.
:::

As of mid-2025, Microsoft split the original single tenant setting into two separate settings. Configure them as follows:

1. Go to the [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal) > **Tenant settings**
2. Under **Developer settings**, enable the applicable setting(s):
   - **Service principals can call Fabric public APIs** — Controls access to CRUD APIs protected by the Fabric permission model (e.g., reading workspaces and items). This is **enabled by default** for new tenants since August 2025.
   - **Service principals can create workspaces, connections, and deployment pipelines** — Controls access to global APIs not protected by Fabric permissions. This is **disabled by default**. Enable only if needed.
3. Restrict access to a dedicated **security group** containing only the service principals that need API access. This is the recommended approach.

:::tip
If you are on an older tenant where the legacy single setting **Service principals can use Fabric APIs** is still visible, enable that instead. It will be automatically migrated to the two new settings.
:::

:::tip
Tenant setting changes can take **up to 15 minutes** to propagate. If you receive 401 errors immediately after enabling, wait and retry.
:::

For detailed instructions, see [Developer admin settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-developer) and [Identity support for Fabric REST APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support).

#### Authentication

The connector supports four authentication methods via the shared `credential` config block. All methods use Azure's `TokenCredential` interface.

##### Service Principal (recommended for production)

Register an application in [Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app) and note the `client_id`, `client_secret`, and `tenant_id`. Then:

1. Ensure the Fabric admin has enabled service principal API access (see **Fabric Admin Settings** above)
2. Create a security group in Entra ID and add the service principal as a member
3. Add the security group as **Contributor** in each target workspace (Contributor role grants access to pipeline definitions and item connections for lineage)

```yaml
credential:
  authentication_method: service_principal
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
```

All three fields are required when using this method.

##### Managed Identity (for Azure-hosted deployments)

Use this when running DataHub ingestion on an Azure VM, AKS, App Service, or other Azure compute that supports [managed identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview). The managed identity must be added as a workspace Contributor in Fabric. A Fabric admin must also enable the tenant settings described in **Fabric Admin Settings** above — these settings govern API access for both service principals and managed identities, despite the setting name referencing only service principals.

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

##### Azure CLI (for local development and testing)

Uses the credentials from your local `az login` session. The signed-in user's existing Fabric permissions apply directly — no additional setup needed beyond workspace access.

```yaml
credential:
  authentication_method: cli
```

Run `az login` before starting ingestion. For remote servers without a browser, use `az login`.

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
2. If using service principal or managed identity:
   - Ensure the Fabric admin has enabled the appropriate developer settings (see **Fabric Admin Settings**)
   - Create a security group, add your identity, and grant **Contributor** on target workspaces
3. If using Azure CLI, run `az login` (or `az login --use-device-code` on remote servers).
4. Configure the ingestion recipe with optional workspace and pipeline filters.


### Install the Plugin
```shell
pip install 'acryl-datahub[fabric-data-factory]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Example recipe for Fabric Data Factory source
# See README.md for full configuration options

source:
  type: fabric-data-factory
  config:
    # Authentication (using service principal)
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Optional: Filter workspaces by name pattern
    workspace_pattern:
      allow:
        - ".*" # Allow all workspaces by default
      deny: []

    # Optional: Filter pipelines by name pattern
    pipeline_pattern:
      allow:
        - ".*" # Allow all pipelines by default
      deny: []

    # Feature flags
    extract_pipelines: true
    include_lineage: true
    include_execution_history: true
    execution_history_days: 7 # 1-90 days

    # Optional: Map Fabric connection names to platform instances for accurate lineage
    # platform_instance_map:
    #   "my-snowflake-connection": "prod_snowflake"
    #   "my-bigquery-connection": "analytics_project"

    # Optional: Platform instance for this Fabric Data Factory connector
    # platform_instance: "my-fabric-tenant"

    # Environment
    env: PROD

    # Optional: Stateful ingestion for stale entity removal
    # stateful_ingestion:
    #   enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: ${DATAHUB_GMS_TOKEN}

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for REST API calls in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">execution_history_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of days of execution history to extract. Only used when include_execution_history is True. Higher values increase ingestion time. Note: Fabric API returns at most 100 recently completed runs per pipeline. <div className="default-line default-line-with-docs">Default: <span className="default-value">7</span></div> |
| <div className="path-line"><span className="path-main">extract_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract Data Pipelines and their activities. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_execution_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract pipeline and activity execution history as DataProcessInstance. Includes run status, duration, and parameters. Enables lineage extraction from parameterized activities using actual runtime values. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract lineage from activity inputs/outputs. Maps Fabric connections to DataHub datasets based on connection type. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">AzureCredentialConfig</span></div> | Unified Azure authentication configuration. <br />  <br /> This class provides a reusable authentication configuration that can be <br /> composed into any Azure connector's configuration. It supports multiple <br /> authentication methods and returns a TokenCredential that works with <br /> any Azure SDK client. <br />  <br /> Example usage in a connector config: <br />     class MyAzureConnectorConfig(ConfigModel): <br />         credential: AzureCredentialConfig = Field( <br />             default_factory=AzureCredentialConfig, <br />             description="Azure authentication configuration" <br />         ) <br />         subscription_id: str = Field(...)  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">authentication_method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "default", "service_principal", "managed_identity", "cli"  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure Application (client) ID. Required for service_principal authentication. Find this in Azure Portal > App registrations > Your app > Overview. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Azure client secret. Required for service_principal authentication. Create in Azure Portal > App registrations > Your app > Certificates & secrets. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">exclude_cli_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude Azure CLI credential. Useful in production to avoid accidentally using developer credentials. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">exclude_environment_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude environment variables. Environment variables checked: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">exclude_managed_identity_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude managed identity. Useful during local development when managed identity is not available. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">managed_identity_client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Client ID for user-assigned managed identity. Leave empty to use system-assigned managed identity. Only used when authentication_method is 'managed_identity'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">tenant_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure tenant (directory) ID. Required for service_principal authentication. Find this in Azure Portal > Microsoft Entra ID > Overview. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">pipeline_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">pipeline_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Configuration for stateful ingestion and stale entity removal. When enabled, tracks ingested entities and removes those that no longer exist in Fabric. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "AzureAuthenticationMethod": {
      "description": "Supported Azure authentication methods.\n\n- DEFAULT: Uses DefaultAzureCredential which auto-detects credentials from\n  environment variables, managed identity, Azure CLI, etc.\n- SERVICE_PRINCIPAL: Uses client ID, client secret, and tenant ID\n- MANAGED_IDENTITY: Uses Azure Managed Identity (system or user-assigned)\n- CLI: Uses Azure CLI credential (requires `az login`)",
      "enum": [
        "default",
        "service_principal",
        "managed_identity",
        "cli"
      ],
      "title": "AzureAuthenticationMethod",
      "type": "string"
    },
    "AzureCredentialConfig": {
      "additionalProperties": false,
      "description": "Unified Azure authentication configuration.\n\nThis class provides a reusable authentication configuration that can be\ncomposed into any Azure connector's configuration. It supports multiple\nauthentication methods and returns a TokenCredential that works with\nany Azure SDK client.\n\nExample usage in a connector config:\n    class MyAzureConnectorConfig(ConfigModel):\n        credential: AzureCredentialConfig = Field(\n            default_factory=AzureCredentialConfig,\n            description=\"Azure authentication configuration\"\n        )\n        subscription_id: str = Field(...)",
      "properties": {
        "authentication_method": {
          "$ref": "#/$defs/AzureAuthenticationMethod",
          "default": "default",
          "description": "Authentication method to use. Options: 'default' (auto-detects from environment), 'service_principal' (client ID + secret + tenant), 'managed_identity' (Azure Managed Identity), 'cli' (Azure CLI credential). Recommended: Use 'default' which tries multiple methods automatically."
        },
        "client_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure Application (client) ID. Required for service_principal authentication. Find this in Azure Portal > App registrations > Your app > Overview.",
          "title": "Client Id"
        },
        "client_secret": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure client secret. Required for service_principal authentication. Create in Azure Portal > App registrations > Your app > Certificates & secrets.",
          "title": "Client Secret"
        },
        "tenant_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure tenant (directory) ID. Required for service_principal authentication. Find this in Azure Portal > Microsoft Entra ID > Overview.",
          "title": "Tenant Id"
        },
        "managed_identity_client_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Client ID for user-assigned managed identity. Leave empty to use system-assigned managed identity. Only used when authentication_method is 'managed_identity'.",
          "title": "Managed Identity Client Id"
        },
        "exclude_cli_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude Azure CLI credential. Useful in production to avoid accidentally using developer credentials.",
          "title": "Exclude Cli Credential",
          "type": "boolean"
        },
        "exclude_environment_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude environment variables. Environment variables checked: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID.",
          "title": "Exclude Environment Credential",
          "type": "boolean"
        },
        "exclude_managed_identity_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude managed identity. Useful during local development when managed identity is not available.",
          "title": "Exclude Managed Identity Credential",
          "type": "boolean"
        }
      },
      "title": "AzureCredentialConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for Fabric Data Factory source.\n\nThis connector extracts metadata from Microsoft Fabric Data Factory items:\n- Workspaces as Containers\n- Data Pipelines as DataFlows with Activities as DataJobs\n- Copy Jobs as DataFlows with dataset-level lineage\n- Dataflow Gen2 as DataFlows (metadata only)",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Configuration for stateful ingestion and stale entity removal. When enabled, tracks ingested entities and removes those that no longer exist in Fabric."
    },
    "credential": {
      "$ref": "#/$defs/AzureCredentialConfig",
      "description": "Azure authentication configuration. Supports service principal, managed identity, Azure CLI, or auto-detection (DefaultAzureCredential). See AzureCredentialConfig for detailed options."
    },
    "workspace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter workspaces by name. Example: allow=['prod-.*'], deny=['.*-test']"
    },
    "pipeline_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter data pipelines by name. Applied to all workspaces matching workspace_pattern."
    },
    "extract_pipelines": {
      "default": true,
      "description": "Whether to extract Data Pipelines and their activities.",
      "title": "Extract Pipelines",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Extract lineage from activity inputs/outputs. Maps Fabric connections to DataHub datasets based on connection type.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "include_execution_history": {
      "default": true,
      "description": "Extract pipeline and activity execution history as DataProcessInstance. Includes run status, duration, and parameters. Enables lineage extraction from parameterized activities using actual runtime values.",
      "title": "Include Execution History",
      "type": "boolean"
    },
    "execution_history_days": {
      "default": 7,
      "description": "Number of days of execution history to extract. Only used when include_execution_history is True. Higher values increase ingestion time. Note: Fabric API returns at most 100 recently completed runs per pipeline.",
      "maximum": 90,
      "minimum": 1,
      "title": "Execution History Days",
      "type": "integer"
    },
    "platform_instance_map": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Map connection names to DataHub platform instances. Example: {'my-snowflake-connection': 'prod_snowflake'}. Used for accurate lineage resolution to existing datasets.",
      "title": "Platform Instance Map",
      "type": "object"
    },
    "api_timeout": {
      "default": 30,
      "description": "Timeout for REST API calls in seconds.",
      "maximum": 300,
      "minimum": 1,
      "title": "Api Timeout",
      "type": "integer"
    }
  },
  "title": "FabricDataFactorySourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage Extraction

##### Which Activities Produce Lineage?

The connector extracts **dataset-level lineage** from these Fabric activity types:

| Activity Type      | Lineage Behavior                                           |
| ------------------ | ---------------------------------------------------------- |
| **Copy**           | Creates lineage from input dataset(s) to output dataset    |
| **InvokePipeline** | Creates pipeline-to-pipeline lineage to the child pipeline |

Lineage is enabled by default (`include_lineage: true`).

##### How Lineage Resolution Works

For lineage to connect properly to datasets ingested from other sources (e.g., Snowflake, BigQuery), the connector resolves Fabric connections to DataHub platforms.

**Step 1: Automatic Connection Mapping**

The connector automatically maps Fabric connection types to DataHub platforms (e.g., a `Snowflake` connection maps to the `snowflake` platform). See [`FABRIC_CONNECTION_PLATFORM_MAP`](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/fabric/common/constants.py) for the full list of supported mappings. Unsupported connection types fall back to using the connection type string as the platform name.

**Step 2: Platform Instance Mapping (for cross-recipe lineage)**

If you're ingesting the same data sources with other DataHub connectors (e.g., Snowflake, BigQuery), you need to ensure the `platform_instance` values match. Use `platform_instance_map` to map your Fabric connection names to the platform instance used in your other recipes:

```yaml
# Fabric Data Factory Recipe
source:
  type: fabric-data-factory
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}
    platform_instance_map:
      # Key: Your Fabric connection name (exact match required)
      # Value: The platform_instance from your other source recipe
      "snowflake-prod-connection": "prod_warehouse"
      "bigquery-analytics": "analytics_project"
```

```yaml
# Corresponding Snowflake Recipe (platform_instance must match)
source:
  type: snowflake
  config:
    platform_instance: "prod_warehouse" # Must match the value in platform_instance_map
    # ... other config
```

Without matching `platform_instance` values, lineage will create separate dataset entities instead of connecting to your existing ingested datasets.

#### Execution History

Pipeline and activity runs are extracted as `DataProcessInstance` entities by default:

```yaml
source:
  type: fabric-data-factory
  config:
    include_execution_history: true # default
    execution_history_days: 7 # 1-90 days
```

This provides run status, duration, timestamps, invoke type, and activity-level details including error messages and retry attempts.

:::note
The Fabric API returns at most 100 recently completed runs per pipeline. Run ingestion more frequently to capture deeper history.
:::

#### Advanced: Multi-Tenant Setup

##### When to Use `platform_instance`

Use the connector's `platform_instance` config to distinguish **separate Fabric tenants** when ingesting from multiple environments:

| Scenario         | Risk                           | Solution     |
| ---------------- | ------------------------------ | ------------ |
| Single tenant    | None                           | Not needed   |
| Multiple tenants | **High** - name collision risk | **Required** |

```yaml
# Multi-tenant example
source:
  type: fabric-data-factory
  config:
    platform_instance: "contoso-tenant" # Prevents URN collisions
```

:::warning
Different Fabric tenants could have identically-named workspaces and pipelines. Use `platform_instance` to prevent entity overwrites.
:::

##### URN Format

Pipeline URNs follow this format:

```
urn:li:dataFlow:(fabric-data-factory,{workspace_id}.{pipeline_id},{env})
```

With `platform_instance`:

```
urn:li:dataFlow:(fabric-data-factory,{platform_instance}.{workspace_id}.{pipeline_id},{env})
```

### Limitations

- **Run history limit**: The Fabric API returns at most 100 recently completed runs per pipeline. If `execution_history_days` covers more runs than this limit, only the most recent 100 are returned. Run ingestion more frequently to capture deeper history.
- **No Dataflow Gen2 support**: Dataflow Gen2 items (standalone workspace-level items with transformation logic) are not extracted.
- **No CopyJob support**: Standalone CopyJob items at the workspace level are not extracted. Only Copy activities embedded within pipelines produce lineage.
- **No trigger/schedule metadata**: Pipeline triggers and schedules are not extracted.
- **ExecutePipeline not supported**: The `ExecutePipeline` activity type is marked as legacy in Fabric and is not supported for cross-pipeline lineage.

#### Lineage

- **Lineage scope**: Only Copy and InvokePipeline activities produce dataset or pipeline lineage. Other activity types (Lookup, Wait, ForEach, Script, etc.) are ingested as DataJobs without dataset-level lineage.
- **InvokePipeline Activity operation types**: Only the `InvokeFabricPipeline` operation type is supported for cross-pipeline lineage. Other operation types (`InvokeAdfPipeline`, `InvokeExternalPipeline`) are not resolved and will be skipped.
- **Query-based Copy sources**: When a Copy activity uses `sqlReaderQuery` or `sqlReaderStoredProcedureName` instead of a direct table reference, lineage is **not extracted**.
- **No column-level lineage**: The connector extracts dataset-level lineage only. Column-to-column mappings from Copy activity translator configurations are not extracted.
- **No Notebook/SparkJobDefinition lineage**: Notebook and SparkJobDefinition activities are ingested as DataJobs but their lineage is not resolved.
- **Connection resolution**: Unmapped connection types fall back to using the connection type string as the platform name, which may not match your existing DataHub platform names. Use `platform_instance_map` to explicitly map connection names.

### Troubleshooting

- **401/403 errors**: Ensure the service principal has the correct Fabric API permissions and is added as a workspace member.
- **Empty results**: Check that `workspace_pattern` and `pipeline_pattern` are not filtering out all items.
- **Missing lineage**: Verify that `include_lineage: true` is set and that Fabric connections are properly configured for the pipelines. Also review the [Lineage limitations](#lineage) section for unsupported activity types and scenarios.
- **Stale entities**: Enable `stateful_ingestion` to automatically remove entities that no longer exist in Fabric.


### Code Coordinates
- Class Name: `datahub.ingestion.source.fabric.data_factory.source.FabricDataFactorySource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/fabric/data_factory/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Fabric Data Factory, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
