### Overview

The `tableau` module ingests metadata from Tableau into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extracts metadata for Sheets, Dashboards, Workbooks, and Data Sources (Embedded and Published) from Tableau projects via the GraphQL Metadata API.

### Prerequisites

- **Tableau Server** 2021.1.10 or later (may work with older versions)
- **Metadata API enabled** ([enable for Server](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server); always enabled for Tableau Cloud)

#### Authentication

DataHub supports two authentication methods:

1. Username/Password
2. [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens)

Either way, the user/token must have at least the **Site Administrator Explorer** site role.

:::info

We need at least the **Site Administrator Explorer** site role in order to get complete metadata from Tableau. Roles with higher privileges, like **Site Administrator Creator** or **Server Admin** also work.

With any lower role, the Tableau Metadata API returns missing/partial metadata.
This particularly affects data source fields and definitions, which impacts our ability to extract most columns and generate column-level lineage. However, table-level lineage will still be extracted for tables even when column metadata is not available (see [Tables Without Column Metadata](#tables-without-column-metadata) section below).
Other site roles, like Viewer or Explorer, are insufficient due to these limitations in the current Tableau Metadata API.

:::
