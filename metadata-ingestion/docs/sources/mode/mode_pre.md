### Overview

The `mode` module ingests metadata from Mode into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Authentication

Generate an API `token` and `password` following Mode's [Authentication documentation](https://mode.com/developer/api-reference/authentication/).

Mode requires a user account for authentication (no service accounts). Consider creating a dedicated user for DataHub ingestion.

#### Permissions

DataHub ingestion requires the user to have the following permissions:

- Have at least the "Member" role.
- For each Connection, have at least"View" access.

  To check Connection permissions, navigate to "Workspace Settings" → "Manage Connections". For each connection in the list, click on the connection → "Permissions". If the default workspace access is "View" or "Query", you're all set for that connection. If it's "Restricted", you'll need to individually grant your ingestion user View access.

- For each Space, have at least "View" access.

  To check Collection permissions, navigate to the "My Collections" page as an Admin user. For each collection with Workspace Access set to "Restricted" access, the ingestion user must be manually granted the "Viewer" access in the "Manage Access" dialog. Collections with "All Members can View/Edit" do not need to be manually granted access.

Note that if the ingestion user has "Admin" access, then it will automatically have "View" access for all connections and collections.
