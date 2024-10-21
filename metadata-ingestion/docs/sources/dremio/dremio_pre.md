### Setup

This integration pulls metadata directly from the Dremio APIs.

You'll need to have a Dremio instance up and running with access to the necessary datasets, and API access should be enabled with a valid token.

**Dremio instance can be one of following**:

    - Dremio Cloud (Fully managed cloud SaaS)
        - Standard
        - Enterprise
    - Dremio Software (self-managed on own infrastructure / on-premise)
        - Community (oss)
        - Enterprise

The API token should have the necessary permissions to **read metadata** and **retrieve lineage**.

#### Steps to Get the Required Information

1. **Generate an API Token**:

   - Log in to your Dremio instance.
   - Navigate to your user profile in the top-right corner.
   - Select **Generate API Token** to create an API token for programmatic access.
   - Ensure that the API token has sufficient permissions to access datasets, spaces, sources, and lineage.

2. **Identify the API Endpoint**:

   - The Dremio API endpoint typically follows this format:  
     `https://<your-dremio-instance>/api/v3/`
   - This endpoint is used to query metadata and lineage information.

3. **Get the Space, Folder, and Dataset Details**:
   - To identify specific datasets or containers (spaces, folders, sources), navigate to the Dremio web interface.
   - Explore the **Spaces** and **Sources** sections to identify the datasets you need to retrieve metadata for.
4. **Permissions**:
   - The token should have **read-only** or **admin** permissions that allow it to:
     - View all datasets (physical and virtual).
     - Access all spaces, folders, and sources.
     - Retrieve dataset and column-level lineage information.
5. **Verify External Data Source Permissions**:
   - If Dremio is connected to external data sources (e.g., AWS S3, relational databases), ensure that Dremio has access to the credentials required for querying those sources.


Ensure your API token has the correct permissions to interact with the Dremio metadata.
