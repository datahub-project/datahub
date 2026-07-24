


# Fabric OneLake

## Overview

Microsoft Fabric OneLake is a storage and lakehouse platform. Learn more in the [official Microsoft Fabric OneLake documentation](https://learn.microsoft.com/fabric/onelake/onelake-overview).

The DataHub integration for Microsoft Fabric OneLake covers workspace, lakehouse, and warehouse containers, table datasets with schema metadata, and view datasets with view definitions and view-to-table lineage parsed from the view SQL. It also extracts query usage statistics from the SQL Analytics Endpoint's `queryinsights` views, and captures stateful deletion detection.

## Concept Mapping

| Microsoft Fabric | DataHub Entity                            | Notes                                                                                 |
| ---------------- | ----------------------------------------- | ------------------------------------------------------------------------------------- |
| **Workspace**    | `Container` (subtype: `Fabric Workspace`) | Top-level organizational unit                                                         |
| **Lakehouse**    | `Container` (subtype: `Fabric Lakehouse`) | Contains schemas and tables                                                           |
| **Warehouse**    | `Container` (subtype: `Fabric Warehouse`) | Contains schemas and tables                                                           |
| **Schema**       | `Container` (subtype: `Fabric Schema`)    | Logical grouping within lakehouse/warehouse                                           |
| **Table**        | `Dataset`                                 | Tables within schemas                                                                 |
| **View**         | `Dataset` (subtype: `View`)               | Lakehouse and Warehouse views; lineage extracted from view definition via SQL parsing |

### Hierarchy Structure

```
Platform (fabric-onelake)
└── Workspace (Container)
    ├── Lakehouse (Container)
    │   └── Schema (Container)
    │       └── Table/View (Dataset)
    └── Warehouse (Container)
        └── Schema (Container)
            └── Table/View (Dataset)
```

### Platform Instance as Tenant

The Fabric REST API does not expose tenant-level endpoints. To represent tenant-level organization in DataHub, set the `platform_instance` configuration field to your tenant identifier (e.g., "contoso-tenant"). This will be included in all container and dataset URNs, effectively grouping all workspaces under the specified platform instance/tenant.


## Module `fabric-onelake`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Extracted from view definitions via SQL parsing when `extract_views` is enabled. |
| Dataset Usage | ✅ | Extracted from queryinsights.exec_requests_history (30-day retention) when `usage.include_usage_statistics` is enabled. Column-level usage is derived via SQL parsing of the query text. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Optionally enabled via `usage.include_usage_statistics` and `usage.include_operational_stats`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |

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
- View datasets with view definitions captured from the SQL Analytics endpoint
- View-to-table lineage parsed from view definitions
- Query usage statistics and operation aspects derived from `queryinsights.exec_requests_history`
- Automatic detection and handling of schemas-enabled and schemas-disabled lakehouses
- Pattern-based filtering for workspaces, lakehouses, warehouses, tables, and views
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

#### SQL Analytics Endpoint Setup

Schema extraction via the SQL Analytics Endpoint requires ODBC drivers to be installed on the system.

##### 1. ODBC Driver Manager

First, install the ODBC driver manager (UnixODBC) on your system:

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y unixodbc unixodbc-dev
```

**RHEL/CentOS/Fedora:**

```bash
# RHEL/CentOS 7/8
sudo yum install -y unixODBC unixODBC-devel

# Fedora / RHEL 9+
sudo dnf install -y unixODBC unixODBC-devel
```

**macOS:**

```bash
brew install unixodbc
```

##### 2. Microsoft ODBC Driver for SQL Server

Install the Microsoft ODBC Driver 18 for SQL Server (required for connecting to Fabric SQL Analytics Endpoint):

**Ubuntu 20.04/22.04:**

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

**RHEL/CentOS 7/8:**

```bash
sudo curl -o /etc/yum.repos.d/mssql-release.repo https://packages.microsoft.com/config/rhel/$(rpm -E %{rhel})/mssql-release.repo
sudo ACCEPT_EULA=Y yum install -y msodbcsql18
```

**RHEL 9 / Fedora:**

```bash
sudo curl -o /etc/yum.repos.d/mssql-release.repo https://packages.microsoft.com/config/rhel/9/mssql-release.repo
sudo ACCEPT_EULA=Y dnf install -y msodbcsql18
```

**macOS:**

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

**Windows:**
Download and install from [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

##### 3. Verify Installation

After installation, verify that the ODBC driver is available:

```bash
odbcinst -q -d
```

You should see `ODBC Driver 18 for SQL Server` in the list.

##### 4. Permissions

Your Azure identity must have access to query the SQL Analytics Endpoint (same permissions as accessing the endpoint via SQL tools).

##### 5. Python Dependencies

The `fabric-onelake` extra includes `sqlalchemy` and `pyodbc` dependencies. Install them with:

```bash
pip install 'acryl-datahub[fabric-onelake]'
```

**Note:** If you encounter `libodbc.so.2: cannot open shared object file` errors, ensure the ODBC driver manager is installed (step 1 above).

#### View Extraction

View extraction reuses the SQL Analytics Endpoint connection from [SQL Analytics Endpoint Setup](#sql-analytics-endpoint-setup) — the same ODBC driver applies, and views are skipped unless `sql_endpoint.enabled` is `true`.

Reading view definitions (needed for view-to-table lineage) requires `VIEW DEFINITION` permission on the SQL Analytics Endpoint. The workspace **Viewer** role used for table ingestion is _not_ sufficient — it grants `db_datareader` only, which causes `INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION` to return `NULL`. There is no workspace-level toggle for this permission; you must choose one of:

- **Grant `VIEW DEFINITION` per Lakehouse/Warehouse** (recommended for least privilege — keeps the identity at workspace Viewer):

  ```sql
  GRANT VIEW DEFINITION ON DATABASE::<lakehouse_or_warehouse_name> TO [<service_principal_name>];
  ```

- **Assign a higher workspace role** (Contributor, Member, or Admin) on the workspaces you ingest.

If neither is acceptable for your environment, set `extract_views: false` to skip view ingestion. Views will still appear without definitions if you ingest them at Viewer level, but lineage will be missing.

References: [Fabric Warehouse roles and permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/share-warehouse-manage-permissions), [Lakehouse workspace roles](https://learn.microsoft.com/en-us/fabric/data-engineering/workspace-roles-lakehouse).

#### Query Usage Statistics

Usage extraction reads [`queryinsights.exec_requests_history`](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights) over the SQL Analytics Endpoint. It reuses the ODBC setup from [SQL Analytics Endpoint Setup](#sql-analytics-endpoint-setup), so `sql_endpoint.enabled` must be `true` — the configuration validator rejects `usage.include_usage_statistics=true` otherwise.

**Required role.** Visibility into `queryinsights` is **scoped per workspace**. The ingesting identity (service principal, managed identity, or user) needs **Contributor or higher** on each workspace whose Lakehouses/Warehouses you want usage for. The workspace **Viewer** role used for table ingestion is _not_ sufficient: per [Microsoft's docs](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights), `queryinsights` requires _"contributor or higher permissions"_ on a Premium-capacity workspace, and full query text — needed for SQL parsing and column-level usage — is only exposed to Admin, Member, and Contributor roles.

**Retention and latency.** Fabric retains `queryinsights` for **30 days only** — older history cannot be backfilled, so configure `usage.start_time` accordingly. Newly executed queries can take up to 15 minutes to appear, increasing under concurrency. System queries and queries from outside a user's context are not surfaced.


### Install the Plugin
```shell
pip install 'acryl-datahub[fabric-onelake]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Example recipe for Microsoft Fabric OneLake source

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
    # This groups all workspaces under a tenant-level container
    # platform_instance: "contoso-tenant"
    
    # Optional: Environment
    # env: PROD
    
    # Optional: Filter workspaces by name pattern
    # workspace_pattern:
    #   allow:
    #     - ".*"  # Allow all workspaces by default
    #   deny: []
    
    # Optional: Filter lakehouses by name pattern
    # lakehouse_pattern:
    #   allow:
    #     - ".*"  # Allow all lakehouses by default
    #   deny: []
    
    # Optional: Filter warehouses by name pattern
    # warehouse_pattern:
    #   allow:
    #     - ".*"  # Allow all warehouses by default
    #   deny: []
    
    # Optional: Filter tables by name pattern
    # Format: 'schema.table' or just 'table' for default schema
    # table_pattern:
    #   allow:
    #     - ".*"  # Allow all tables by default
    #   deny: []

    # Optional: Filter views by name pattern
    # Format: 'schema.view' or just 'view' for default schema
    # view_pattern:
    #   allow:
    #     - ".*"  # Allow all views by default
    #   deny: []

    # Feature flags
    extract_lakehouses: true
    extract_warehouses: true
    extract_schemas: true  # Set to false to skip schema containers
    extract_views: true    # Requires sql_endpoint.enabled
    
    # Optional: API timeout (seconds)
    # api_timeout: 30
    
    # Optional: Stateful ingestion for stale entity removal
    # stateful_ingestion:
    #   enabled: true
    #   remove_stale_metadata: true
    
    # Optional: Schema extraction configuration
    # extract_schema:
    #   enabled: true  # Enable schema extraction (default: true)
    #   method: sql_analytics_endpoint
    
    # Optional: SQL Analytics Endpoint configuration
    # sql_endpoint:
    #   enabled: true  # Enable SQL endpoint connection (default: true)
    #   odbc_driver: "ODBC Driver 18 for SQL Server"  # ODBC driver name (default: "ODBC Driver 18 for SQL Server")
    #   encrypt: "yes"  # Enable encryption (default: "yes")
    #   trust_server_certificate: "no"  # Trust server certificate (default: "no")
    #   query_timeout: 30  # Timeout for SQL queries in seconds (default: 30)

    # Optional: Query usage extraction. Reads queryinsights.exec_requests_history
    # on each Lakehouse/Warehouse SQL Analytics Endpoint. Requires sql_endpoint.enabled=true.
    # Fabric retains queryinsights for 30 days.
    # usage:
    #   include_usage_statistics: true   # Master toggle (default: true)
    #   skip_failed_queries: true        # Filter rows where status != 'Succeeded' (default: true)
    #   include_operational_stats: true  # Emit per-query operation aspects (default: true)
    #   include_top_n_queries: true
    #   top_n_queries: 10
    #   bucket_duration: DAY
    #   # start_time: "2026-04-01T00:00:00Z"
    #   # end_time:   "2026-05-01T00:00:00Z"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for REST API calls in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lakehouses</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract lakehouses and their tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_schemas</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract schema containers. If False, tables will be directly under lakehouse/warehouse containers. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract views and their definitions. Requires a configured sql_endpoint, because views are discovered via INFORMATION_SCHEMA.VIEWS over the SQL Analytics Endpoint. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_warehouses</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract warehouses and their tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
| <div className="path-line"><span className="path-main">extract_schema</span></div> <div className="type-name-line"><span className="type-name">ExtractSchemaConfig</span></div> | Configuration for schema extraction.  |
| <div className="path-line"><span className="path-prefix">extract_schema.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable schema extraction <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">extract_schema.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Schema extraction method. Currently only 'sql_analytics_endpoint' is supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">sql&#95;analytics&#95;endpoint</span></div> |
| <div className="path-line"><span className="path-main">lakehouse_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">lakehouse_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sql_endpoint</span></div> <div className="type-name-line"><span className="type-name">One of SqlEndpointConfig, null</span></div> | SQL Analytics Endpoint configuration. Required when extract_views=True or when extract_schema.enabled=True with method='sql_analytics_endpoint'.  |
| <div className="path-line"><span className="path-prefix">sql_endpoint.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable SQL Analytics Endpoint connection <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">sql_endpoint.</span><span className="path-main">encrypt</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "yes", "no", "mandatory", "optional", "strict" <div className="default-line default-line-with-docs">Default: <span className="default-value">yes</span></div> |
| <div className="path-line"><span className="path-prefix">sql_endpoint.</span><span className="path-main">odbc_driver</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | ODBC driver name for SQL Server connections. <div className="default-line default-line-with-docs">Default: <span className="default-value">ODBC Driver 18 for SQL Server</span></div> |
| <div className="path-line"><span className="path-prefix">sql_endpoint.</span><span className="path-main">query_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for SQL queries in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-prefix">sql_endpoint.</span><span className="path-main">trust_server_certificate</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "yes", "no" <div className="default-line default-line-with-docs">Default: <span className="default-value">no</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">usage</span></div> <div className="type-name-line"><span className="type-name">FabricUsageConfig</span></div> | Usage tracking configuration for Fabric OneLake. <br />  <br /> Retention is 30 days per Microsoft Fabric documentation: <br /> https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, emit a `Query` entity for each unique parsed queryinsights row so the SQL becomes a first-class, searchable asset in DataHub (visible on the Queries tab and as standalone Query pages). Requires `include_usage_statistics=True`. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Master toggle for Fabric usage extraction. When False, no `datasetUsageStatistics` or `operation` aspects are emitted from queryinsights, regardless of `include_operational_stats`. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">skip_failed_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True, the SQL filter excludes rows where status != 'Succeeded' (canceled / failed queries are skipped at the source). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">usage.user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">warehouse_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "ExtractSchemaConfig": {
      "additionalProperties": false,
      "description": "Configuration for schema extraction.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Enable schema extraction",
          "title": "Enabled",
          "type": "boolean"
        },
        "method": {
          "const": "sql_analytics_endpoint",
          "default": "sql_analytics_endpoint",
          "description": "Schema extraction method. Currently only 'sql_analytics_endpoint' is supported.",
          "title": "Method",
          "type": "string"
        }
      },
      "title": "ExtractSchemaConfig",
      "type": "object"
    },
    "FabricUsageConfig": {
      "additionalProperties": false,
      "description": "Usage tracking configuration for Fabric OneLake.\n\nRetention is 30 days per Microsoft Fabric documentation:\nhttps://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights",
      "properties": {
        "bucket_duration": {
          "$ref": "#/$defs/BucketDuration",
          "default": "DAY",
          "description": "Size of the time window to aggregate usage stats."
        },
        "end_time": {
          "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
          "format": "date-time",
          "title": "End Time",
          "type": "string"
        },
        "start_time": {
          "default": null,
          "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
          "format": "date-time",
          "title": "Start Time",
          "type": "string"
        },
        "top_n_queries": {
          "default": 10,
          "description": "Number of top queries to save to each table.",
          "exclusiveMinimum": 0,
          "title": "Top N Queries",
          "type": "integer"
        },
        "user_email_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "regex patterns for user emails to filter in usage."
        },
        "include_operational_stats": {
          "default": true,
          "description": "Whether to display operational stats.",
          "title": "Include Operational Stats",
          "type": "boolean"
        },
        "include_read_operational_stats": {
          "default": false,
          "description": "Whether to report read operational stats. Experimental.",
          "title": "Include Read Operational Stats",
          "type": "boolean"
        },
        "format_sql_queries": {
          "default": false,
          "description": "Whether to format sql queries",
          "title": "Format Sql Queries",
          "type": "boolean"
        },
        "include_top_n_queries": {
          "default": true,
          "description": "Whether to ingest the top_n_queries.",
          "title": "Include Top N Queries",
          "type": "boolean"
        },
        "include_usage_statistics": {
          "default": true,
          "description": "Master toggle for Fabric usage extraction. When False, no `datasetUsageStatistics` or `operation` aspects are emitted from queryinsights, regardless of `include_operational_stats`.",
          "title": "Include Usage Statistics",
          "type": "boolean"
        },
        "skip_failed_queries": {
          "default": true,
          "description": "When True, the SQL filter excludes rows where status != 'Succeeded' (canceled / failed queries are skipped at the source).",
          "title": "Skip Failed Queries",
          "type": "boolean"
        },
        "include_queries": {
          "default": true,
          "description": "If enabled, emit a `Query` entity for each unique parsed queryinsights row so the SQL becomes a first-class, searchable asset in DataHub (visible on the Queries tab and as standalone Query pages). Requires `include_usage_statistics=True`.",
          "title": "Include Queries",
          "type": "boolean"
        }
      },
      "title": "FabricUsageConfig",
      "type": "object"
    },
    "SqlEndpointConfig": {
      "additionalProperties": false,
      "description": "Configuration for SQL Analytics Endpoint schema extraction.\n\nReferences:\n- https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-connectivity\n- https://learn.microsoft.com/en-us/fabric/data-warehouse/connect-to-fabric-data-warehouse\n- https://learn.microsoft.com/en-us/fabric/data-warehouse/what-is-the-sql-analytics-endpoint-for-a-lakehouse\n- https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17#encrypt",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Enable SQL Analytics Endpoint connection",
          "title": "Enabled",
          "type": "boolean"
        },
        "odbc_driver": {
          "default": "ODBC Driver 18 for SQL Server",
          "description": "ODBC driver name for SQL Server connections.",
          "title": "Odbc Driver",
          "type": "string"
        },
        "encrypt": {
          "default": "yes",
          "description": "Enable encryption for SQL Server connections. Valid values: 'yes'/'mandatory' (enable encryption, default in ODBC Driver 18.0+), 'no'/'optional' (disable encryption), or 'strict' (ODBC Driver 18.0+, TDS 8.0 protocol only, always verifies server certificate). See: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17#encrypt",
          "enum": [
            "yes",
            "no",
            "mandatory",
            "optional",
            "strict"
          ],
          "title": "Encrypt",
          "type": "string"
        },
        "trust_server_certificate": {
          "default": "no",
          "description": "Trust server certificate without validation. Set to 'yes' only if certificate validation fails. When 'encrypt=strict', this setting is ignored and certificate validation is always performed. See: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17",
          "enum": [
            "yes",
            "no"
          ],
          "title": "Trust Server Certificate",
          "type": "string"
        },
        "query_timeout": {
          "default": 30,
          "description": "Timeout for SQL queries in seconds",
          "maximum": 300,
          "minimum": 1,
          "title": "Query Timeout",
          "type": "integer"
        }
      },
      "title": "SqlEndpointConfig",
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
  "description": "Configuration for Fabric OneLake source.\n\nThis connector extracts metadata from Microsoft Fabric OneLake including:\n- Workspaces as Containers\n- Lakehouses as Containers\n- Warehouses as Containers\n- Schemas as Containers\n- Tables as Datasets with schema metadata\n\nNote on Tenant/Platform Instance:\nThe Fabric REST API does not expose tenant-level endpoints or operations.\nAll API operations are performed at the workspace level. To represent tenant-level\norganization in DataHub, users should set the `platform_instance` configuration\nfield to their tenant identifier (e.g., \"contoso-tenant\"). This will be included\nin all container and dataset URNs, effectively grouping all workspaces under the\nspecified platform instance/tenant.",
  "properties": {
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
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
    "lakehouse_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter lakehouses by name. Applied to all workspaces matching workspace_pattern."
    },
    "warehouse_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter warehouses by name. Applied to all workspaces matching workspace_pattern."
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter tables by name. Format: 'schema.table' or just 'table' for default schema."
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter views by name. Format: 'schema.view_name' or just 'view_name' for default schema."
    },
    "extract_lakehouses": {
      "default": true,
      "description": "Whether to extract lakehouses and their tables.",
      "title": "Extract Lakehouses",
      "type": "boolean"
    },
    "extract_warehouses": {
      "default": true,
      "description": "Whether to extract warehouses and their tables.",
      "title": "Extract Warehouses",
      "type": "boolean"
    },
    "extract_views": {
      "default": true,
      "description": "Whether to extract views and their definitions. Requires a configured sql_endpoint, because views are discovered via INFORMATION_SCHEMA.VIEWS over the SQL Analytics Endpoint.",
      "title": "Extract Views",
      "type": "boolean"
    },
    "extract_schemas": {
      "default": true,
      "description": "Whether to extract schema containers. If False, tables will be directly under lakehouse/warehouse containers.",
      "title": "Extract Schemas",
      "type": "boolean"
    },
    "api_timeout": {
      "default": 30,
      "description": "Timeout for REST API calls in seconds.",
      "maximum": 300,
      "minimum": 1,
      "title": "Api Timeout",
      "type": "integer"
    },
    "extract_schema": {
      "$ref": "#/$defs/ExtractSchemaConfig",
      "description": "Configuration for schema extraction from tables."
    },
    "sql_endpoint": {
      "anyOf": [
        {
          "$ref": "#/$defs/SqlEndpointConfig"
        },
        {
          "type": "null"
        }
      ],
      "description": "SQL Analytics Endpoint configuration. Required when extract_views=True or when extract_schema.enabled=True with method='sql_analytics_endpoint'."
    },
    "usage": {
      "$ref": "#/$defs/FabricUsageConfig",
      "description": "Usage tracking configuration. Reads `queryinsights.exec_requests_history` on each Lakehouse/Warehouse SQL Analytics Endpoint. Requires a configured and enabled `sql_endpoint`."
    }
  },
  "title": "FabricOneLakeSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Basic Recipe

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

#### Advanced Configuration

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

    view_pattern:
      allow:
        - ".*"
      deny:
        - ".*_internal"

    # Feature flags
    extract_lakehouses: true
    extract_warehouses: true
    extract_schemas: true # Set to false to skip schema containers
    extract_views: true # Requires sql_endpoint.enabled

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

#### Using Managed Identity

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

#### Using Azure CLI (Local Development)

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

#### Schema Extraction

Schema extraction (column metadata) is supported via the SQL Analytics Endpoint. This feature extracts column names, data types, nullability, and ordinal positions from tables in both Lakehouses and Warehouses.

See [SQL Analytics Endpoint Setup](#sql-analytics-endpoint-setup) under Prerequisites for ODBC driver installation.

#### Schema Extraction Configuration

Schema extraction is enabled by default. You can configure it as follows:

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Schema extraction configuration
    extract_schema:
      enabled: true # Enable schema extraction (default: true)
      method: sql_analytics_endpoint # Currently only this method is supported

    # SQL Analytics Endpoint configuration
    sql_endpoint:
      enabled: true # Enable SQL endpoint connection (default: true)
      # Optional: ODBC connection options
      # odbc_driver: "ODBC Driver 18 for SQL Server"  # Default: "ODBC Driver 18 for SQL Server"
      # encrypt: "yes"  # Enable encryption (default: "yes")
      # trust_server_certificate: "no"  # Trust server certificate (default: "no")
      query_timeout: 30 # Timeout for SQL queries in seconds (default: 30)
```

#### How It Works

1. **Endpoint Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone.
2. **Authentication**: Uses the same Azure credentials configured for REST API access with Azure AD token injection
3. **Connection**: Connects to the SQL Analytics Endpoint using ODBC with the discovered endpoint URL
4. **Query**: Queries `INFORMATION_SCHEMA.COLUMNS` to extract column metadata (required for schema extraction)
5. **Type Mapping**: SQL Server data types are automatically mapped to DataHub types using the standard type mapping system

**References:**

- [What is the SQL analytics endpoint for a lakehouse?](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
- [Warehouse connectivity in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)
- [Connect to Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/how-to-connect)

#### Important Notes

- **Endpoint URL Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone. If the endpoint URL cannot be retrieved from the API, schema extraction will fail for that item.
- **No Fallback**: Unlike legacy Power BI Premium endpoints, Fabric SQL Analytics Endpoints do not support fallback connection strings. The endpoint must be obtained from the API.

#### Disabling Schema Extraction

To disable schema extraction and ingest tables without column metadata:

```yaml
source:
  type: fabric-onelake
  config:
    extract_schema:
      enabled: false
```

#### View Extraction

Views in Lakehouses and Warehouses are ingested as DataHub `Dataset` entities with the `View` subtype. Each view dataset includes:

- Column-level schema metadata (sourced from `INFORMATION_SCHEMA.COLUMNS` alongside table columns).
- The original view definition (`CREATE VIEW` SQL), captured from `INFORMATION_SCHEMA.VIEWS`.
- Upstream table lineage parsed from the view definition via the SQL parsing aggregator.

See [View Extraction](#view-extraction) under Prerequisites for required ODBC setup and the `VIEW DEFINITION` permission needed to read view definitions.

##### Configuration

```yaml
source:
  type: fabric-onelake
  config:
    # View extraction is enabled by default. Set to false to skip views.
    extract_views: true

    # Filter views by name pattern. Format: 'schema.view' or just 'view' for default schema.
    view_pattern:
      allow:
        - ".*"
      deny:
        - ".*_internal"

    # View extraction requires the SQL Analytics Endpoint (enabled by default).
    sql_endpoint:
      enabled: true
```

##### How It Works

1. **Discovery**: The connector queries `INFORMATION_SCHEMA.VIEWS` on the SQL Analytics Endpoint to list views and capture their definitions.
2. **Filtering**: Each view is matched against `view_pattern` using the `schema.view_name` form.
3. **Schema**: Column metadata is reused from the same `INFORMATION_SCHEMA.COLUMNS` query that powers table schema extraction — no extra queries per view.
4. **Lineage**: View definitions are passed to the SQL parsing aggregator to derive view → upstream table lineage. View URNs and upstream table URNs are resolved within the same workspace and item.

#### Usage Statistics

The connector extracts query usage statistics from each Lakehouse and Warehouse by reading the [`queryinsights.exec_requests_history`](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights) view on the SQL Analytics Endpoint. Each captured query is parsed by the SQL parsing aggregator and emitted as:

- `datasetUsageStatistics` aspects — query counts, distinct user counts, top users, top fields, and (when enabled) top SQL queries, bucketed by the configured window.
- `operation` aspects — per-query operation events (insert, update, delete, etc.) when `usage.include_operational_stats` is enabled.

See [Query Usage Statistics](#query-usage-statistics) under Prerequisites for the required workspace role (Contributor or higher) and ODBC setup.

##### Configuration

```yaml
source:
  type: fabric-onelake
  config:
    # Usage extraction is enabled by default. Set to false to skip query usage.
    usage:
      include_usage_statistics: true

      # When true, the SQL filter excludes rows where status != 'Succeeded'
      # (canceled / failed queries are skipped at the source).
      skip_failed_queries: true

      # Optional: emit per-query operation aspects in addition to aggregated
      # datasetUsageStatistics. Defaults to true (inherited from BaseUsageConfig).
      include_operational_stats: true

      # Optional: include top SQL queries in the usage payload.
      include_top_n_queries: true
      top_n_queries: 10

      # Optional: window the connector queries from queryinsights. Defaults to
      # the standard BaseUsageConfig "last bucket" window. Fabric retains
      # queryinsights for 30 days.
      bucket_duration: DAY
      # start_time: "2026-04-01T00:00:00Z"
      # end_time:   "2026-05-01T00:00:00Z"

    # Usage extraction depends on the SQL Analytics Endpoint.
    extract_schema:
      enabled: true
    sql_endpoint:
      enabled: true
```

All standard `BaseUsageConfig` fields (`bucket_duration`, `start_time`, `end_time`, `top_n_queries`, `format_sql_queries`, `include_top_n_queries`, `include_operational_stats`, `user_email_pattern`, etc.) are supported under the `usage` block.

When stateful ingestion is enabled, the usage time window is checkpointed only after a successful run, so a partial or failed run won't silently skip the next window.

#### Schemas-Enabled vs Schemas-Disabled Lakehouses

The connector automatically handles both schemas-enabled and schemas-disabled lakehouses:

- **Schemas-Enabled Lakehouses**: The connector uses OneLake Delta Table APIs to list schemas first, then tables within each schema. This requires Storage audience tokens (`https://storage.azure.com/.default`).
- **Schemas-Disabled Lakehouses**: The connector uses the standard Fabric REST API `/tables` endpoint, which lists all tables. Tables without an explicit schema are automatically assigned to the `dbo` schema in DataHub. This uses Power BI API scope tokens.

**Important**: All tables in DataHub will have a schema in their URN, even for schemas-disabled lakehouses. Tables without an explicit schema are normalized to use the `dbo` schema by default. This ensures consistent URN structure across all Fabric entities.

The connector automatically detects the lakehouse type and uses the appropriate API endpoint. No configuration changes are needed.

#### Stateful Ingestion

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- **Metadata Sync Delays**: The SQL Analytics Endpoint may have delays in reflecting schema changes. New columns or schema modifications may take minutes to hours to appear.
- **Missing Tables**: Some tables may not be visible in the SQL endpoint due to:
  - Unsupported data types
  - Permission issues
  - Table count limits in very large databases
- **Graceful Degradation**: If schema extraction fails for a table, the table will still be ingested without column metadata (no ingestion failure)
- **View Extraction Requires SQL Endpoint**: Views are only discovered through the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, or if the endpoint is unreachable for a given Lakehouse/Warehouse, views in that item will not be ingested.
- **Usage Statistics Retention**: Fabric `queryinsights` retains query history for only **30 days**. Older usage cannot be backfilled, regardless of the configured `usage.start_time`.
- **Usage Statistics Requires SQL Endpoint**: Usage extraction reads `queryinsights.exec_requests_history` over the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, the configuration validator will reject `usage.include_usage_statistics=true`. If the endpoint is unreachable for a specific Lakehouse/Warehouse, usage for that item is skipped without failing the run.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.fabric.onelake.source.FabricOneLakeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/fabric/onelake/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Fabric OneLake, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
