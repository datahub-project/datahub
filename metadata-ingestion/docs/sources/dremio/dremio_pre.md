### Setup

This integration pulls metadata directly from the Dremio APIs.

You'll need to have a Dremio instance up and running with access to the necessary datasets, and API access should be enabled with a valid token.

The API token should have the necessary permissions to **read metadata** and **retrieve lineage**.

#### Steps to Get the Required Information

1. **Generate an API Token**:

   - Log in to your Dremio instance.
   - Navigate to your user profile in the top-right corner.
   - Select **Generate API Token** to create an API token for programmatic access.

2. **Permissions**:

   - The token should have **read-only** or **admin** permissions that allow it to:
     - View all datasets (physical and virtual).
     - Access all spaces, folders, and sources.
     - Retrieve dataset and column-level lineage information.

3. **Verify External Data Source Permissions**:
   - If Dremio is connected to external data sources (e.g., AWS S3, relational databases), ensure that Dremio has access to the credentials required for querying those sources.
