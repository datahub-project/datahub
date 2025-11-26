### Prerequisites

In order to execute this source, the user credentials need the following privileges:

- `GRANT SELECT ON DATABASE.* TO 'USERNAME'`
- `GRANT SHOW VIEW ON DATABASE.* TO 'USERNAME'`

`SELECT` is required to see the table structure as well as for profiling.

### Foreign Data Wrapper (FDW) Support

DataHub supports ingesting Foreign Data Wrapper (FDW) tables from PostgreSQL. FDW tables allow PostgreSQL to access external data sources (such as SQL Server, Oracle, MySQL, MongoDB, S3, etc.) as if they were local tables.

#### What are Foreign Tables?

Foreign tables in PostgreSQL are virtual tables that reference data stored in external systems. They are created using Foreign Data Wrappers, which act as adapters between PostgreSQL and external data sources.

**Common FDW Types**:

- `postgres_fdw` - Connect to other PostgreSQL databases
- `oracle_fdw` - Connect to Oracle databases
- `tds_fdw` - Connect to SQL Server/Sybase databases
- `mysql_fdw` - Connect to MySQL/MariaDB databases
- `mongo_fdw` - Connect to MongoDB
- `file_fdw` - Read CSV/text files
- `s3_fdw` - Access data in Amazon S3

#### Configuration

Foreign table ingestion is **enabled by default**. You can control it using these configuration options:

```yml
source:
  type: postgres
  config:
    # ... connection details ...

    # Include/exclude foreign tables (default: true)
    include_foreign_tables: true

    # Filter foreign tables by pattern (default: allow all)
    foreign_table_pattern:
      allow:
        - ".*" # Allow all foreign tables
      deny:
        - "temp_.*" # Exclude temporary foreign tables
```

#### Filtering Foreign Tables

Foreign tables must match **both** `table_pattern` and `foreign_table_pattern` to be ingested:

```yml
source:
  type: postgres
  config:
    # Regular table pattern
    table_pattern:
      allow:
        - "public\..*"  # Only public schema

    # Foreign table pattern (additional filtering)
    foreign_table_pattern:
      allow:
        - ".*_external"  # Only foreign tables ending with _external
```

#### FDW Metadata

DataHub captures the following FDW-specific metadata as custom properties:

- `is_foreign_table`: Always set to `"true"` for foreign tables
- `fdw_type`: The Foreign Data Wrapper type (e.g., `postgres_fdw`, `oracle_fdw`, `tds_fdw`)
- `fdw_server`: The name of the foreign server
- `fdw_server_options`: Server connection options (e.g., host, port, dbname)
- `fdw_table_options`: Table-specific options (e.g., schema_name, table_name)

**Example Custom Properties**:

```json
{
  "is_foreign_table": "true",
  "fdw_type": "tds_fdw",
  "fdw_server": "sqlserver_prod",
  "fdw_server_options": "host=sqlserver.example.com, port=1433, dbname=sales",
  "fdw_table_options": "schema_name=dbo, table_name=customers"
}
```

#### Features

- **FDW-Agnostic**: Works with any FDW type (existing or future)
- **Graceful Handling**: Handles missing or incomplete FDW metadata
- **Schema Support**: Works across all schemas, not just `public`
- **Special Characters**: Supports table names with special characters
- **Separate Processing**: Foreign tables are processed after regular tables
- **Backward Compatible**: Additive feature that doesn't affect existing ingestion

#### Limitations

- **Profiling**: Data profiling is disabled for foreign tables (metadata only)
- **Permissions**: Requires `SELECT` privilege on `pg_class`, `pg_foreign_table`, `pg_foreign_server`, and `pg_foreign_data_wrapper` system catalogs

#### Example Configuration

**Basic Configuration** (includes foreign tables):

```yml
source:
  type: postgres
  config:
    host_port: "postgres.example.com:5432"
    database: "production"
    username: "${POSTGRES_USER}"
    password: "${POSTGRES_PASSWORD}"
    # Foreign tables included by default
```

**Exclude Foreign Tables**:

```yml
source:
  type: postgres
  config:
    host_port: "postgres.example.com:5432"
    database: "production"
    username: "${POSTGRES_USER}"
    password: "${POSTGRES_PASSWORD}"
    include_foreign_tables: false
```

**Filter Foreign Tables by Pattern**:

```yml
source:
  type: postgres
  config:
    host_port: "postgres.example.com:5432"
    database: "production"
    username: "${POSTGRES_USER}"
    password: "${POSTGRES_PASSWORD}"
    include_foreign_tables: true
    foreign_table_pattern:
      allow:
        - "public\..*"  # Only public schema
        - "analytics\.external_.*"  # External tables in analytics schema
      deny:
        - ".*_temp"  # Exclude temporary tables
```

### AWS RDS IAM Authentication

For AWS RDS PostgreSQL instances, you can use IAM authentication instead of traditional username/password authentication.

**Setup:**

Follow the [AWS RDS IAM Database Authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) documentation to:

- Enable IAM database authentication on your RDS instance
- Create database users that use IAM authentication
- Configure IAM policies with `rds-db:connect` permissions

**Configuration:**

Set `auth_mode: "AWS_IAM"` in your recipe and optionally configure `aws_config` for AWS credentials and region. If `aws_config` is not specified, boto3 will automatically use the default credential chain from environment variables, AWS config files, or IAM role metadata.

```yml
source:
  type: postgres
  config:
    host_port: "mydb.abc123.us-east-1.rds.amazonaws.com:5432"
    database: "production"
    username: "datahub_user"
    auth_mode: "AWS_IAM"
    aws_config:
      aws_region: "us-east-1"
      # Optional: specify credentials explicitly
      # aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      # aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```
