### Overview

The `unity-catalog` module ingests metadata from Databricks into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

- Get your Databricks instance's [workspace url](https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
- Create a [Databricks Service Principal](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#what-is-a-service-principal)
  - You can skip this step and use your own account to get things running quickly,
    but we strongly recommend creating a dedicated service principal for production use.

#### Authentication Options

You can authenticate with Databricks using OAuth, Azure authentication, a Personal Access Token (legacy), or Databricks unified authentication:

**Option 1: OAuth**

- Generate a Databricks OAuth secret using the following guide:
  - [Authorize service principal access to Databricks with OAuth](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m#oauth-m2m-manual)

**Option 2: Azure Authentication (for Azure Databricks)**

- Create an Azure Active Directory application:
  - Follow the [Azure AD app registration guide](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
  - Note down the `client_id` (Application ID), `tenant_id` (Directory ID), and create a `client_secret`
- Grant the Azure AD application access to your Databricks workspace:
  - Add the service principal to your Databricks workspace following [this guide](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#add-a-service-principal-to-your-azure-databricks-account-using-the-account-console)

**Option 3: Personal Access Token (PAT) (legacy)**

- Generate a Databricks Personal Access token following the following guides:
  - [Service Principals](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#personal-access-tokens)
  - [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-tokens)

**Option 4: Unified authentication**

- Set authentication configuration via environment variables or Databricks configuration profiles, as specified in the [Databricks unified authentication guide](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth).

#### Provision your service account:

- To ingest your workspace's metadata and lineage, your service principal must have all of the following:
  - One of: metastore admin role, ownership of, or `USE CATALOG` privilege on any catalogs you want to ingest
  - One of: metastore admin role, ownership of, or `USE SCHEMA` privilege on any schemas you want to ingest
  - Ownership of or `SELECT` privilege on any tables and views you want to ingest
  - [Ownership documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html)
  - [Privileges documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- To ingest legacy hive_metastore catalog (`include_hive_metastore` - enabled by default), your service principal must have all of the following:
  - `READ_METADATA` and `USAGE` privilege on `hive_metastore` catalog
  - `READ_METADATA` and `USAGE` privilege on schemas you want to ingest
  - `READ_METADATA` and `USAGE` privilege on tables and views you want to ingest
  - [Hive Metastore Privileges documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges-hms.html)
- To ingest your workspace's notebooks and respective lineage, your service principal must have `CAN_READ` privileges on the folders containing the notebooks you want to ingest: [guide](https://docs.databricks.com/en/security/auth-authz/access-control/workspace-acl.html#folder-permissions).
- To `include_usage_statistics` (enabled by default), your service principal must have one of the following:
  - `CAN_MANAGE` permissions on any SQL Warehouses you want to ingest: [guide](https://docs.databricks.com/security/auth-authz/access-control/sql-endpoint-acl.html).
  - When `usage_data_source` is set to `SYSTEM_TABLES` or `AUTO` (default) with `warehouse_id` configured: `SELECT` privilege on `system.query.history` table for improved performance with large query volumes and multi-workspace setups.
- To ingest `profiling` information with `method: ge`, you need `SELECT` privileges on all profiled tables.
- To ingest `profiling` information with `method: analyze` and `call_analyze: true` (enabled by default), your service principal must have ownership or `MODIFY` privilege on any tables you want to profile.
  - Alternatively, you can run [ANALYZE TABLE](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) yourself on any tables you want to profile, then set `call_analyze` to `false`.
    You will still need `SELECT` privilege on those tables to fetch the results.
- Check the starter recipe below and replace `workspace_url` and either `token` (for PAT authentication) or `azure_auth` credentials (for Azure authentication) with your information from the previous steps.

#### Permissions for DataHub Cloud Assertions (Observe)

If you plan to use DataHub Cloud's [Freshness](/docs/managed-datahub/observe/freshness-assertions), [Volume](/docs/managed-datahub/observe/volume-assertions), or [Column](/docs/managed-datahub/observe/column-assertions) Assertions on Databricks, the required Unity Catalog privileges depend on which **Source** you select in the assertion builder:

| Source Type                                                                        | Required Privilege(s)                                                                                              | Notes                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Table Statistics**                                                               | `MODIFY` (or ownership) on the target table                                                                        | Runs `ANALYZE TABLE ... COMPUTE STATISTICS` followed by `DESCRIBE TABLE EXTENDED`. On Delta tables this is metadata-only (reads file-level stats from the transaction log). Tables only, not Views. Default Volume Source. |
| **Information Schema**                                                             | `USE CATALOG` + `USE SCHEMA` on the containing catalog/schema, plus `SELECT` on `system.information_schema.tables` | Queries the Unity Catalog `information_schema.tables` view. Tables only, not Views.                                                                                                                                        |
| **Audit Log**                                                                      | `SELECT` on `system.access.audit` (requires Unity Catalog system schemas to be enabled)                            | Reads workspace audit events. Tables only.                                                                                                                                                                                 |
| **File Metadata**                                                                  | `SELECT` on the target table                                                                                       | Reads file-level modification time via Delta transaction log metadata. Delta tables only.                                                                                                                                  |
| **Query** / **Last Modified Column** / **High Watermark Column** / **Field Value** | `SELECT` on the target table                                                                                       | Runs SQL queries against the table. Works for Tables and Views.                                                                                                                                                            |
| **DataHub Operation** / **DataHub Dataset Profile**                                | _(none)_                                                                                                           | Uses DataHub metadata only, no Databricks access needed.                                                                                                                                                                   |

In addition, the service principal used for assertion evaluation needs `USE CATALOG` and `USE SCHEMA` on the catalog and schema containing the target tables, and must be granted access to a SQL Warehouse (`CAN_USE` permission) to run statements.
