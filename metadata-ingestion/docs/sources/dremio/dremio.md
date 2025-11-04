### Configuration for Dremio Cloud

```yaml
source:
  type: dremio
  config:
    # Authentication details
    authentication_method: PAT # Use Personal Access Token for authentication
    password: <your_api_token> # Replace <your_api_token> with your Dremio Cloud API token
    is_dremio_cloud: True # Set to True for Dremio Cloud instances
    dremio_cloud_project_id: <project_id> # Provide the Project ID for Dremio Cloud

    # Enable query lineage tracking
    include_query_lineage: True

    # Optional: Map Dremio sources to external platforms for lineage
    source_mappings:
      - platform: s3
        source_name: samples

    # Optional: Filter datasets using schema patterns
    schema_pattern:
      allow:
        - "<source_name>.<table_name>"

sink:
  # Define your sink configuration here
```

### Required Permissions

For Dremio Cloud instances, configure the following project-level privileges for your user account:

```sql
-- Required: Access to containers and their metadata
GRANT READ METADATA ON <project_name> TO USER <username>

-- Required: Access to system tables for dataset metadata
GRANT SELECT ON <project_name> TO USER <username>

-- Optional: For data profiling (only if profiling is enabled)
GRANT SELECT ON SOURCE <source_name> TO USER <username>
GRANT SELECT ON SPACE <space_name> TO USER <username>

-- Optional: For query lineage (only if include_query_lineage: true)
GRANT VIEW JOB HISTORY ON <project_name> TO USER <username>
```

**Notes**:

- In Dremio Cloud, replace `SYSTEM` with your project name when granting privileges
- `READ METADATA` is required for container discovery via API calls
- `SELECT ON <project_name>` is required for accessing system tables
