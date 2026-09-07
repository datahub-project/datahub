### Overview

This integration extracts metadata from Airbyte's API to capture information about your connections, sources, destinations, and the lineage between them.

### Prerequisites

You'll need to have an Airbyte instance running with configured sources and destinations, and access to the Airbyte API.

#### Steps to Get the Required Information

1. **Determine Your Deployment Type**:

   - **Open Source (OSS)**: If you're running a self-hosted Airbyte instance
   - **Cloud**: If you're using Airbyte Cloud

2. **Authentication Credentials**:

   - **For Open Source (OSS)**:

     - The URL of your Airbyte instance (host and port)
     - **OAuth2 client credentials** (Airbyte 1.0+) - obtain via:
       - UI: Navigate to **User > User settings > Applications** to create an application and copy credentials
       - CLI: Run `abctl local credentials` (abctl v0.11.0+)
     - Username and password if basic authentication is enabled
     - API token if available

   - **For Airbyte Cloud**:
     - OAuth2 client ID and client secret (required)
     - OAuth2 refresh token (optional — omit to use `client_credentials` grant; provide to use `refresh_token` grant)
     - Your Airbyte Cloud workspace ID

3. **API Access**:

   - For OSS users, ensure the API is accessible at `/api/public/v1` path prefix
   - Verify connectivity by testing the health endpoint: `http://localhost:8000/api/public/v1/health`
   - Ensure you have proper network connectivity between your DataHub instance and the Airbyte API

4. **Permissions**:
   - The authentication credentials should have permissions to:
     - Read workspace information
     - List and read sources, destinations, and connections
     - Access connection schemas and sync catalogs
     - View job execution history (if extracting job statuses)

#### Airbyte Version for Stream Namespaces

Airbyte's Public API returns a connection's stream list without the schema (namespace) each
stream is read from. DataHub recovers it from the `/streams` endpoint. Two things depend on it:

- **Dataset URNs.** Without a namespace, every stream falls back to the source connector's
  configured default schema, so streams living in other schemas resolve to the wrong table.
  Namespaces are only reported from **Airbyte 1.7.0 onwards** — older deployments answer
  `/streams` without any namespace field, and the run warns that namespaces were not reported.
  Set `default_schema` per source under `sources_to_platform_instance` to supply one manually.
- **Column-level lineage**, which is built from the field list `/streams` returns. This works on
  older versions too, since the field list does not depend on namespace support.
