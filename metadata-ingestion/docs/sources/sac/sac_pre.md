### Overview

The `sac` module ingests metadata from SAP Analytics Cloud (SAC) into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer to [Manage OAuth Clients](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/00f68c2e08b941f081002fd3691d86a7/4f43b54398fc4acaa5efa32badfe3df6.html) to create an OAuth client in SAP Analytics Cloud. The OAuth client is required to have the following properties:

   - Purpose: API Access
   - Access:
     - Story Listing
     - Data Import Service
     - Data Export Service (only required when the opt-in `ingest_acquired_data_model_schema_metadata` is enabled - the schema of acquired Data Models is then read via the Data Export Service)
     - File Repository Read (only required when the opt-in `ingest_private_content` is enabled - used to read each resource's `folderType` so public and private root folders are labeled correctly)
   - Authorization Grant: Client Credentials

2. Maintain connection mappings (optional):

To map individual connections in SAP Analytics Cloud to platforms, platform instances and environments, the `connection_mapping` configuration can be used within the recipe:

```yaml
connection_mapping:
  MY_BW_CONNECTION:
    platform: bw
    platform_instance: PROD_BW
    env: PROD
  MY_HANA_CONNECTION:
    platform: hana
    platform_instance: PROD_HANA
    env: PROD
```

The key in the connection mapping dictionary represents the name of the connection created in SAP Analytics Cloud.

#### Content scope: public vs. non-public content

By default the connector ingests only **public** content (stories and analytic applications saved
in SAC's `Public` folder). This matches what most users browse under **Files → My Files → Public**
and requires only the `Story Listing` access grant. If that is all you need, no further configuration
is required.

To also ingest **non-public** content (private `My Files` and team folders), enable
`ingest_private_content`. What you actually get back depends on the OAuth client's permissions,
because SAC scopes results to the requesting principal:

| Mode | Recipe | OAuth client permissions | What is ingested |
| --- | --- | --- | --- |
| **Public only** (default) | _(none)_ | `Story Listing` | All public stories and applications. |
| **What the client can see** | `ingest_private_content: true` | `Story Listing` + `File Repository Read` | Public content **plus** any private/team content directly shared with the OAuth client. A client-credentials client owns no personal content, so in practice this adds only content explicitly shared with it. |
| **Tenant-wide** | `ingest_private_content: true`<br/>`apply_manage_privilege: true` | `Story Listing` + `File Repository Read` + a role with the **Manage** privilege on **Private Files** and/or **Public Files** | Public content **plus all** private and team content on the tenant (SAC's administrative `System` view). |

`apply_manage_privilege` appends `applyManagePrivilege=true` to the resource requests. Without the
Manage privilege on the OAuth client it has no effect, so combine it with the permission grant above.

When `ingest_private_content` is enabled the connector reads each resource's `folderType` from the
SAC File Repository API (`/api/v1/filerepository/Resources`) so the built-in root folders are
labeled by type rather than by their language-localized display name. The canonical labels are configurable via `public_root_folder_name` (default `Public`) and
`private_root_folder_name` (default `My Files`). Team folders and other roots keep their given names.
If the File Repository API is unavailable, or a public resource is missing from it (a permission
skew between `Story Listing` and `File Repository Read`, or paging truncation), public roots are
still canonicalized from each resource's own `isPublic` flag; only non-public roots then fall back to
their raw localized names, and ingestion continues.
