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

The connector requires **read-only access** to Fabric workspaces and their contents. Permission requirements differ based on the authentication method:

#### For Service Principals and Managed Identities

Service principals and managed identities do **not** use API permission scopes. According to [Microsoft's documentation](https://learn.microsoft.com/en-us/rest/api/fabric/articles/scopes), scopes only apply to delegated (interactive user) access. Instead, service principals require:

**1. Fabric Tenant Setting (Admin Configuration):**

Enable service principal API access in the Fabric Admin Portal:

- Navigate to **Fabric Admin Portal** → **Tenant Settings**
- Enable **"Service principals can use Fabric APIs"**
- Add your service principal to the allowed list (or enable for entire organization)

**2. Workspace Role Assignment (Choose One Approach):**

**Recommended: Contributor/Member Role (Full Access)**

- Assign the service principal **Contributor** or **Member** role in each workspace
- ✅ Full read access to workspaces, lakehouses, warehouses, and tables
- ✅ Can access SQL Analytics Endpoint for schema extraction
- ✅ Bypasses OneLake security (no additional configuration needed)
- ✅ Simplest setup (workspace-wide access)

**Alternative: Viewer Role + Per-Item Sharing (Granular Control)**

If you need read-only access with fine-grained control:

1. Assign the service principal **Viewer** role in the workspace

2. For each lakehouse/warehouse, grant item-level permissions:

   - **Option A:** Share with **"Read all data using SQL"** (ReadData permission)

     - ✅ Can access SQL Analytics Endpoint for schema extraction
     - ✅ Can read all tables via SQL queries
     - Requires per-item sharing (not workspace-wide)

   - **Option B:** Share with **"Read all Apache Spark data"** (ReadAll permission)
     - ✅ Can list tables via Fabric REST API
     - ✅ Automatically gets DefaultReader OneLake security role
     - ❌ **Cannot extract schemas** (Viewer role blocks SQL Analytics Endpoint in default Delegated mode)
     - Use only if schema extraction is disabled (`extract_schema.enabled: false`)

**Permission Comparison Table:**

| Workspace Role         | Item Permission | SQL Endpoint Access | Table Access    | Schema Extraction | Setup Complexity          |
| ---------------------- | --------------- | ------------------- | --------------- | ----------------- | ------------------------- |
| **Contributor/Member** | (none needed)   | ✅ Yes              | ✅ All tables   | ✅ Yes            | Low (workspace-wide)      |
| **Viewer**             | ReadData        | ✅ Yes              | ✅ All tables   | ✅ Yes            | Medium (per-item sharing) |
| **Viewer**             | ReadAll         | ❌ No               | ✅ Via REST API | ❌ No             | Medium (per-item sharing) |

**Note on SQL Analytics Endpoint Access:**

Schema extraction requires SQL Analytics Endpoint access. Service principals access the SQL endpoint using [Delegated Identity Mode](https://learn.microsoft.com/en-us/fabric/onelake/sql-analytics-endpoint-onelake-security) (the default mode), where the endpoint connects to OneLake using the workspace/item owner's identity, and access is controlled by workspace roles and item permissions:

- **Contributor/Member/Admin** workspace roles → ✅ SQL endpoint access granted
- **Viewer** role alone → ❌ SQL endpoint access denied
- **Viewer + ReadData** item permission → ✅ SQL endpoint access granted (via item sharing)

**Note:** Microsoft Fabric supports an alternative "User Identity Mode" for SQL endpoints, which passes through the calling user's identity to OneLake. However, this mode is only available for interactive users and cannot be used with service principals. Service principals must use Delegated Identity Mode with the permissions listed above.

#### For Interactive User Authentication (Azure CLI, DefaultAzureCredential)

Interactive user authentication uses delegated permissions:

**1. Microsoft Entra Delegated Permissions:**

- `Workspace.Read.All` (delegated) - Required to list and read workspace metadata
- Or `Workspace.ReadWrite.All` (delegated) - Provides read and write access

**2. Workspace Role Assignment:**

- User must have **Viewer** role or higher in the Fabric workspaces to ingest

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

### Granting Permissions

**For Service Principal:**

1. **Register an application** in Microsoft Entra ID (Azure AD)

   - Navigate to **Azure Portal** → **App registrations** → **New registration**
   - Note the **Application (client) ID** and **Directory (tenant) ID**
   - Create a **Client secret** under **Certificates & secrets**

2. **Enable Fabric API access** (Fabric Admin required)

   - Navigate to **Fabric Admin Portal** → **Tenant Settings**
   - Enable **"Service principals can use Fabric APIs"**
   - Add your service principal's Application ID to the allowed list

3. **Assign workspace roles** (Choose one approach):

   **Recommended Approach: Contributor Role**

   - In Fabric portal, navigate to each workspace
   - Go to **Workspace settings** → **Access**
   - Add your service principal (search by Application ID or name)
   - Assign **Contributor** role
   - Repeat for all workspaces to ingest

   **Alternative Approach: Viewer + Item Sharing**

   - Assign **Viewer** role in workspace (as above)
   - For each lakehouse/warehouse:
     - Click **Share** or **Manage permissions**
     - Add the service principal
     - Select **"Read all data using SQL"** (for schema extraction)
     - Click **Grant access**
   - Repeat for all lakehouses/warehouses to ingest

**For Managed Identity:**

1. **Enable managed identity** on your Azure resource (VM, AKS, App Service, etc.)

   - For system-assigned: Enable in resource settings
   - For user-assigned: Create and assign managed identity resource
   - Note the **Object (principal) ID**

2. **Enable Fabric API access** (Fabric Admin required)

   - Navigate to **Fabric Admin Portal** → **Tenant Settings**
   - Enable **"Service principals can use Fabric APIs"**
   - Add the managed identity's Object ID to the allowed list

3. **Assign workspace roles** (same as service principal)
   - Use **Contributor** role (recommended) or **Viewer + item sharing**
   - The connector will automatically use the managed identity for authentication

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

Schema extraction (column metadata) is supported via the SQL Analytics Endpoint. This feature extracts column names, data types, nullability, and ordinal positions from tables in both Lakehouses and Warehouses.

### Prerequisites for Schema Extraction

Schema extraction via SQL Analytics Endpoint requires ODBC drivers to be installed on the system.

#### 1. ODBC Driver Manager

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

#### 2. Microsoft ODBC Driver for SQL Server

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

#### 3. Verify Installation

After installation, verify that the ODBC driver is available:

```bash
odbcinst -q -d
```

You should see `ODBC Driver 18 for SQL Server` in the list.

#### 4. Permissions

Your Azure identity must have access to query the SQL Analytics Endpoint (same permissions as accessing the endpoint via SQL tools).

#### 5. Python Dependencies

The `fabric-onelake` extra includes `sqlalchemy` and `pyodbc` dependencies. Install them with:

```bash
pip install 'acryl-datahub[fabric-onelake]'
```

**Note:** If you encounter `libodbc.so.2: cannot open shared object file` errors, ensure the ODBC driver manager is installed (step 1 above).

### Configuration

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

### How It Works

1. **Endpoint Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone.
2. **Authentication**: Uses the same Azure credentials configured for REST API access with Azure AD token injection
3. **Connection**: Connects to the SQL Analytics Endpoint using ODBC with the discovered endpoint URL
4. **Query**: Queries `INFORMATION_SCHEMA.COLUMNS` to extract column metadata (required for schema extraction)
5. **Type Mapping**: SQL Server data types are automatically mapped to DataHub types using the standard type mapping system

**References:**

- [What is the SQL analytics endpoint for a lakehouse?](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
- [Warehouse connectivity in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)
- [Connect to Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/how-to-connect)

### Important Notes

- **Endpoint URL Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone. If the endpoint URL cannot be retrieved from the API, schema extraction will fail for that item.
- **No Fallback**: Unlike legacy Power BI Premium endpoints, Fabric SQL Analytics Endpoints do not support fallback connection strings. The endpoint must be obtained from the API.

### Known Limitations

- **Metadata Sync Delays**: The SQL Analytics Endpoint may have delays in reflecting schema changes. New columns or schema modifications may take minutes to hours to appear.
- **Missing Tables**: Some tables may not be visible in the SQL endpoint due to:
  - Unsupported data types
  - Permission issues
  - Table count limits in very large databases
- **Graceful Degradation**: If schema extraction fails for a table, the table will still be ingested without column metadata (no ingestion failure)

### Disabling Schema Extraction

To disable schema extraction and ingest tables without column metadata:

```yaml
source:
  type: fabric-onelake
  config:
    extract_schema:
      enabled: false
```

## Schemas-Enabled vs Schemas-Disabled Lakehouses

The connector automatically handles both schemas-enabled and schemas-disabled lakehouses:

- **Schemas-Enabled Lakehouses**: The connector uses OneLake Delta Table APIs to list schemas first, then tables within each schema. This requires Storage audience tokens (`https://storage.azure.com/.default`).
- **Schemas-Disabled Lakehouses**: The connector uses the standard Fabric REST API `/tables` endpoint, which lists all tables. Tables without an explicit schema are automatically assigned to the `dbo` schema in DataHub. This uses Power BI API scope tokens.

**Important**: All tables in DataHub will have a schema in their URN, even for schemas-disabled lakehouses. Tables without an explicit schema are normalized to use the `dbo` schema by default. This ensures consistent URN structure across all Fabric entities.

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
- [OneLake Overview](https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview)
- [Workspaces in Fabric](https://learn.microsoft.com/en-us/fabric/get-started/workspaces)
- [Lakehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Warehouses in Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing)
