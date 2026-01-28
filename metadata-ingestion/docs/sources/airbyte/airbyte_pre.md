### Setup

This integration extracts metadata from Airbyte's API to capture information about your connections, sources, destinations, and the lineage between them.

You'll need to have an Airbyte instance running with configured sources and destinations, and access to the Airbyte API.

#### Steps to Get the Required Information

1. **Determine Your Deployment Type**:

   - **Open Source (OSS)**: If you're running a self-hosted Airbyte instance
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

4. **Permissions**:
   - The authentication credentials should have permissions to:
     - Read workspace information
     - List and read sources, destinations, and connections
     - Access connection schemas and sync catalogs
     - View job execution history (if extracting job statuses)
