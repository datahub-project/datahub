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

#### Prerequisites for Schema Extraction

Schema extraction via SQL Analytics Endpoint requires ODBC drivers to be installed on the system.

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

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
