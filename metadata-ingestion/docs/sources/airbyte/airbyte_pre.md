### Setup

This integration extracts metadata from Airbyte's API to capture information about your connections, sources, destinations, and the lineage between them.

You'll need to have an Airbyte instance running with configured sources and destinations, and access to the Airbyte API.

#### Steps to Get the Required Information

1. **Determine Your Deployment Type**:

   - **Open Source**: If you're running a self-hosted Airbyte instance
   - **Cloud**: If you're using Airbyte Cloud

2. **Authentication Credentials**:

   - **For Open Source (OSS)**:

     - The URL of your Airbyte instance (host and port)
     - Username and password if basic authentication is enabled
     - API token if available

   - **For Airbyte Cloud**:
     - OAuth2 client ID
     - OAuth2 client secret
     - OAuth2 refresh token
     - Your Airbyte Cloud workspace ID

3. **API Access**:
   - For OSS users, ensure the API is accessible at `/api/public/v1` path prefix
   - Verify connectivity by testing the health endpoint: `http://localhost:8000/api/public/v1/health`
   - Ensure you have proper network connectivity between your DataHub instance and the Airbyte API

### Starter Recipe for Airbyte OSS Instance

```yaml
source:
  type: airbyte
  config:
    # Authentication details for OSS deployment
    deployment_type: oss
    host_port: http://localhost:8000 # The URL of your Airbyte instance
    username: airbyte # Replace with your Airbyte username
    password: password # Replace with your Airbyte password

    # Enable column-level lineage tracking
    extract_column_level_lineage: true

    # Include connection job statuses
    include_statuses: true

    # Optional filtering
    # source_pattern:
    #   allow:
    #     - ".*MySQL.*"                  # Only include MySQL sources
    # destination_pattern:
    #   allow:
    #     - ".*Postgres.*"               # Only include Postgres destinations

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

### Starter Recipe for Airbyte Cloud Instance

```yaml
source:
  type: airbyte
  config:
    # Authentication details for Cloud deployment
    deployment_type: cloud
    oauth2_client_id: # Replace with your OAuth2 client ID
    oauth2_client_secret: # Replace with your OAuth2 client secret
    oauth2_refresh_token: # Replace with your OAuth2 refresh token
    cloud_workspace_id: # Replace with your Airbyte Cloud workspace ID

    # Enable column-level lineage tracking
    extract_column_level_lineage: true

    # Include connection job statuses
    include_statuses: true

    # Optional: Extract ownership information
    extract_owners: true
    owner_extraction_pattern: ".*owner:([\\w-]+).*" # Extract owner from connection name

    # Optional: Extract tags from metadata
    extract_tags: true

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```
