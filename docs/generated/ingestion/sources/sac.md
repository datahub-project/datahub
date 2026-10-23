


# SAP Analytics Cloud

## Overview

SAP Analytics Cloud is a business intelligence and analytics platform. Learn more in the [official SAP Analytics Cloud documentation](https://www.sap.com/products/technology-platform/analytics-cloud.html).

The DataHub integration for SAP Analytics Cloud covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

| SAP Analytics Cloud | DataHub     |
| ------------------- | ----------- |
| `Story`             | `Dashboard` |
| `Application`       | `Dashboard` |
| `Live Data Model`   | `Dataset`   |
| `Import Data Model` | `Dataset`   |
| `Model`             | `Dataset`   |


## Module `sac`
![Beta](https://img.shields.io/badge/support%20status-Beta-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default for Import Data Models; acquired Data Models are opt-in via ingest_acquired_data_model_schema_metadata. |
| Table-Level Lineage | ✅ | Enabled by default (only for Live Data Models). |

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

| Mode                        | Recipe                                                            | OAuth client permissions                                                                                                     | What is ingested                                                                                                                                                                                               |
| --------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Public only** (default)   | _(none)_                                                          | `Story Listing`                                                                                                              | All public stories and applications.                                                                                                                                                                           |
| **What the client can see** | `ingest_private_content: true`                                    | `Story Listing` + `File Repository Read`                                                                                     | Public content **plus** any private/team content directly shared with the OAuth client. A client-credentials client owns no personal content, so in practice this adds only content explicitly shared with it. |
| **Tenant-wide**             | `ingest_private_content: true`<br/>`apply_manage_privilege: true` | `Story Listing` + `File Repository Read` + a role with the **Manage** privilege on **Private Files** and/or **Public Files** | Public content **plus all** private and team content on the tenant (SAC's administrative `System` view).                                                                                                       |

`apply_manage_privilege` appends `applyManagePrivilege=true` to the resource requests. Without the
Manage privilege on the OAuth client it has no effect, so combine it with the permission grant above.

When `ingest_private_content` is enabled the connector reads each resource's `folderType` from the
SAC File Repository API (`/api/v1/filerepository/Resources`) so the built-in root folders are
labeled by type rather than by their language-localized display name. The canonical labels are configurable via `public_root_folder_name` (default `Public`) and
`private_root_folder_name` (default `My Files`). Team folders and other roots keep their given names.

All private content shares a single `private_root_folder_name` root. When `apply_manage_privilege`
surfaces other users' private content, their stories are grouped under the same `My Files` browse tree
rather than a per-user folder; each asset's owner is still recorded individually via its `createdBy`
ownership.

If the File Repository API is unavailable, or a public resource is missing from it (a permission
skew between `Story Listing` and `File Repository Read`, or paging truncation), public roots are
still canonicalized from each resource's own `isPublic` flag; only non-public roots then fall back to
their raw localized names, and ingestion continues.


### Install the Plugin
```shell
pip install 'acryl-datahub[sac]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
    type: sac
    config:
        stateful_ingestion:
            enabled: true

        tenant_url: # Your SAP Analytics Cloud tenant URL, e.g. https://company.eu10.sapanalytics.cloud or https://company.eu10.hcs.cloud.sap
        token_url: # The Token URL of your SAP Analytics Cloud tenant, e.g. https://company.eu10.hana.ondemand.com/oauth/token.

        # Add secret in Secrets Tab with relevant names for each variable
        client_id: "${SAC_CLIENT_ID}" # Your SAP Analytics Cloud client id
        client_secret: "${SAC_CLIENT_SECRET}" # Your SAP Analytics Cloud client secret

        # ingest stories
        ingest_stories: true

        # ingest applications
        ingest_applications: true

        resource_id_pattern:
            allow:
                - .*

        resource_name_pattern:
            allow:
                - .*

        # SAC returns its built-in public root folder in each resource's browse path
        # localized to the resource language (e.g. "Public" / "Öffentlich"). The connector
        # rewrites that first segment to this canonical label so all public content shares
        # one browse tree; set to null to keep the raw localized name.
        public_root_folder_name: Public

        # Only a first browse-path segment matching one of these labels (case-insensitive)
        # is treated as the public root and rewritten. Other shared roots that SAC can also
        # return (e.g. a team/workspace root like "Default") are left untouched. Add your
        # tenant language's public-root label if it is not already listed.
        public_root_folder_aliases:
            - Public
            - Öffentlich

        # folder_pattern is matched against the canonicalized browse path, so write rules
        # against public_root_folder_name (e.g. Public/...) rather than a localized root.
        folder_pattern:
            allow:
                - .*

        connection_mapping:
            MY_BW_CONNECTION:
                platform: bw
                platform_instance: PROD_BW
                env: PROD
            MY_HANA_CONNECTION:
                platform: hana
                platform_instance: PROD_HANA
                env: PROD
            # SAP Datasphere (DWC) connection: SAC does not expose the Datasphere space,
            # so set it here to build the upstream sap-datasphere lineage (<space>.<model_name>).
            MY_DATASPHERE_CONNECTION:
                datasphere_space: my_space
                # platform_instance / env / convert_urns_to_lowercase must match how the
                # SAP Datasphere connector ingested the assets so the urns stitch.
                env: PROD
                convert_urns_to_lowercase: true

        # Emit column-level lineage to SAP Datasphere by mirroring the upstream dataset's
        # schema from DataHub (requires a datahub_api/graph connection and SAP Datasphere
        # to be ingested first). Falls back to table-level lineage when unavailable.
        resolve_datasphere_column_lineage: true

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client ID for the OAuth authentication  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Client secret for the OAuth authentication  |
| <div className="path-line"><span className="path-main">tenant_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | URL of the SAP Analytics Cloud tenant  |
| <div className="path-line"><span className="path-main">token_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | URL of the OAuth token endpoint of the SAP Analytics Cloud tenant  |
| <div className="path-line"><span className="path-main">apply_manage_privilege</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Append `applyManagePrivilege=true` to resource requests, which asks SAC to return all content on the tenant (the administrative `System` view) instead of only what the OAuth client can see directly. This requires the OAuth client to be assigned a role with the Manage privilege on Private Files and/or Public Files; without that privilege the parameter has no effect. Intended to be combined with `ingest_private_content` to ingest non-public content tenant-wide. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_acquired_data_model_schema_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether schema metadata of acquired (non-import) Data Models is ingested via the Data Export Service. Live Data Models keep their schema in the source system and are skipped. Ingesting this schema adds one metadata request per acquired model. Requires the 'Data Export Service' access grant on the SAC OAuth client (see Prerequisites). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_applications</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether Analytic Applications should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_import_data_model_schema_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether schema metadata of Import Data Models should be ingested (ingesting schema metadata of Import Data Models significantly increases overall ingestion time) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_private_content</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | By default the connector ingests only public content (`isPublic eq 1`). When enabled, the `isPublic` filter is dropped so private and team content the OAuth client can see is ingested as well. With a client-credentials OAuth client (a technical user that owns no content), this alone typically surfaces little beyond public content unless `apply_manage_privilege` is also enabled and the client holds the Manage privilege on Private/Public Files. When enabled, the connector additionally reads each resource's `folderType` from the File Repository API to canonicalize root folders correctly (public vs. private) rather than assuming everything is public. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_stories</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether Stories should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_root_folder_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Canonical display name for SAC's built-in private (`My Files`) root folder. Only relevant when `ingest_private_content` is enabled; the first `ancestorPath` segment of a resource whose `folderType` is `PRIVATE` is rewritten to this value (the same localization dedup as `public_root_folder_name`, applied to the private root). Set to null to keep the raw localized name. <div className="default-line default-line-with-docs">Default: <span className="default-value">My Files</span></div> |
| <div className="path-line"><span className="path-main">public_root_folder_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Canonical display name for SAC's built-in public root folder. SAC returns this root in each resource's `ancestorPath` localized to the resource's language (e.g. 'Public' in English, 'Öffentlich' in German), which would otherwise split the same physical root into one browse-path tree per language. Only a first `ancestorPath` segment matching `public_root_folder_aliases` is rewritten to this value; other roots are left untouched. User-created sub-folders keep their given name. Set to null to disable rewriting and keep the raw localized name. <div className="default-line default-line-with-docs">Default: <span className="default-value">Public</span></div> |
| <div className="path-line"><span className="path-main">query_name_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating dataset urns of consumed queries, the placeholder {query} can be used within the template for inserting the name of the query <div className="default-line default-line-with-docs">Default: <span className="default-value">QUERY/&#123;name&#125;</span></div> |
| <div className="path-line"><span className="path-main">resolve_datasphere_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | In addition to table-level DWC lineage, emit column-level lineage to the backing SAP Datasphere dataset. SAC does not expose columns for Live Data Models, so the field list is resolved from the upstream Datasphere dataset's schema in DataHub (requires a `datahub_api`/graph connection) and mirrored onto the SAC dataset, since the live model is a passthrough. Falls back to table-level lineage when the graph or upstream schema is unavailable. No effect unless `resolve_datasphere_lineage` is also enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">resolve_datasphere_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | For SAC Live Data Models backed by SAP Datasphere (Data Warehouse Cloud / 'DWC' connections), emit upstream lineage to the backing SAP Datasphere dataset. The Datasphere object's technical name is derived from the SAC model name; the Datasphere space is not exposed by SAC and must be supplied via `connection_mapping.<connection_id>.datasphere_space`. The upstream urn is built as `<space>.<model_name>`. SAC disambiguates repeated links to the same Datasphere object with a trailing numeric suffix that the real object does not carry; when a `datahub_api`/graph connection is available this is corrected by matching the suffix-trimmed name against an ingested Datasphere dataset. Models on connections without a configured `datasphere_space` are skipped with a warning. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionMappingConfig)</span></div> |   |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lower-case identifiers when constructing the upstream dataset urn for this connection. Must match the `convert_urns_to_lowercase` setting used by the corresponding upstream connector recipe so the urns stitch. Currently applied to SAP Datasphere ('DWC') upstreams only; BW/HANA upstreams preserve case as before. Defaults to True (matching the SAP Datasphere connector default). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">datasphere_space</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | For SAP Datasphere ('DWC') connections only: the Datasphere space id that backs this connection (e.g. `bdap_sac`). SAC does not expose the space for Datasphere-backed live models, so it must be supplied here to build the upstream sap-datasphere dataset urn (`<space>.<model_name>`). Leave unset for non-Datasphere connections. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The platform that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">public_root_folder_aliases</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Root folder display names that identify SAC's built-in public root across tenant languages. `isPublic eq 1` can also return other shared roots (e.g. a team/workspace root such as 'Default'), so only a first `ancestorPath` segment matching one of these labels (case-insensitive) is rewritten to `public_root_folder_name` — any other root is preserved rather than merged into the public tree. Add your tenant language's label if it is not listed. When `ingest_private_content` is enabled and a resource's `folderType` is known from the File Repository, that type is authoritative and the public root is canonicalized regardless of this list; the aliases only gate the fallback where `folderType` is unavailable. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;Public&#x27;, &#x27;Öffentlich&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">public_root_folder_aliases.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">resource_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">resource_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">resource_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">resource_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion related configs <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "ConnectionMappingConfig": {
      "additionalProperties": false,
      "properties": {
        "env": {
          "default": "PROD",
          "description": "The environment that this connection mapping belongs to",
          "title": "Env",
          "type": "string"
        },
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The platform that this connection mapping belongs to",
          "title": "Platform"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that this connection mapping belongs to",
          "title": "Platform Instance"
        },
        "datasphere_space": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "For SAP Datasphere ('DWC') connections only: the Datasphere space id that backs this connection (e.g. `bdap_sac`). SAC does not expose the space for Datasphere-backed live models, so it must be supplied here to build the upstream sap-datasphere dataset urn (`<space>.<model_name>`). Leave unset for non-Datasphere connections.",
          "title": "Datasphere Space"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Whether to lower-case identifiers when constructing the upstream dataset urn for this connection. Must match the `convert_urns_to_lowercase` setting used by the corresponding upstream connector recipe so the urns stitch. Currently applied to SAP Datasphere ('DWC') upstreams only; BW/HANA upstreams preserve case as before. Defaults to True (matching the SAP Datasphere connector default).",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "ConnectionMappingConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion related configs"
    },
    "tenant_url": {
      "description": "URL of the SAP Analytics Cloud tenant",
      "title": "Tenant Url",
      "type": "string"
    },
    "token_url": {
      "description": "URL of the OAuth token endpoint of the SAP Analytics Cloud tenant",
      "title": "Token Url",
      "type": "string"
    },
    "client_id": {
      "description": "Client ID for the OAuth authentication",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Client secret for the OAuth authentication",
      "format": "password",
      "title": "Client Secret",
      "type": "string",
      "writeOnly": true
    },
    "ingest_stories": {
      "default": true,
      "description": "Controls whether Stories should be ingested",
      "title": "Ingest Stories",
      "type": "boolean"
    },
    "ingest_applications": {
      "default": true,
      "description": "Controls whether Analytic Applications should be ingested",
      "title": "Ingest Applications",
      "type": "boolean"
    },
    "ingest_import_data_model_schema_metadata": {
      "default": true,
      "description": "Controls whether schema metadata of Import Data Models should be ingested (ingesting schema metadata of Import Data Models significantly increases overall ingestion time)",
      "title": "Ingest Import Data Model Schema Metadata",
      "type": "boolean"
    },
    "ingest_acquired_data_model_schema_metadata": {
      "default": false,
      "description": "Controls whether schema metadata of acquired (non-import) Data Models is ingested via the Data Export Service. Live Data Models keep their schema in the source system and are skipped. Ingesting this schema adds one metadata request per acquired model. Requires the 'Data Export Service' access grant on the SAC OAuth client (see Prerequisites).",
      "title": "Ingest Acquired Data Model Schema Metadata",
      "type": "boolean"
    },
    "resource_id_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting resource ids that are to be included"
    },
    "resource_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting resource names that are to be included"
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting folders that are to be included. Matched against the browse path whose built-in root folder has already been canonicalized (e.g. `Public/...`), not SAC's raw localized `ancestorPath`; write rules against the canonical `public_root_folder_name` value rather than a language-specific root such as `\u00d6ffentlich`. Only story and application browse paths are matched; model browse paths (the synthetic `Models` root) are not filtered."
    },
    "public_root_folder_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "Public",
      "description": "Canonical display name for SAC's built-in public root folder. SAC returns this root in each resource's `ancestorPath` localized to the resource's language (e.g. 'Public' in English, '\u00d6ffentlich' in German), which would otherwise split the same physical root into one browse-path tree per language. Only a first `ancestorPath` segment matching `public_root_folder_aliases` is rewritten to this value; other roots are left untouched. User-created sub-folders keep their given name. Set to null to disable rewriting and keep the raw localized name.",
      "title": "Public Root Folder Name"
    },
    "public_root_folder_aliases": {
      "default": [
        "Public",
        "\u00d6ffentlich"
      ],
      "description": "Root folder display names that identify SAC's built-in public root across tenant languages. `isPublic eq 1` can also return other shared roots (e.g. a team/workspace root such as 'Default'), so only a first `ancestorPath` segment matching one of these labels (case-insensitive) is rewritten to `public_root_folder_name` \u2014 any other root is preserved rather than merged into the public tree. Add your tenant language's label if it is not listed. When `ingest_private_content` is enabled and a resource's `folderType` is known from the File Repository, that type is authoritative and the public root is canonicalized regardless of this list; the aliases only gate the fallback where `folderType` is unavailable.",
      "items": {
        "type": "string"
      },
      "title": "Public Root Folder Aliases",
      "type": "array"
    },
    "private_root_folder_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "My Files",
      "description": "Canonical display name for SAC's built-in private (`My Files`) root folder. Only relevant when `ingest_private_content` is enabled; the first `ancestorPath` segment of a resource whose `folderType` is `PRIVATE` is rewritten to this value (the same localization dedup as `public_root_folder_name`, applied to the private root). Set to null to keep the raw localized name.",
      "title": "Private Root Folder Name"
    },
    "ingest_private_content": {
      "default": false,
      "description": "By default the connector ingests only public content (`isPublic eq 1`). When enabled, the `isPublic` filter is dropped so private and team content the OAuth client can see is ingested as well. With a client-credentials OAuth client (a technical user that owns no content), this alone typically surfaces little beyond public content unless `apply_manage_privilege` is also enabled and the client holds the Manage privilege on Private/Public Files. When enabled, the connector additionally reads each resource's `folderType` from the File Repository API to canonicalize root folders correctly (public vs. private) rather than assuming everything is public.",
      "title": "Ingest Private Content",
      "type": "boolean"
    },
    "apply_manage_privilege": {
      "default": false,
      "description": "Append `applyManagePrivilege=true` to resource requests, which asks SAC to return all content on the tenant (the administrative `System` view) instead of only what the OAuth client can see directly. This requires the OAuth client to be assigned a role with the Manage privilege on Private Files and/or Public Files; without that privilege the parameter has no effect. Intended to be combined with `ingest_private_content` to ingest non-public content tenant-wide.",
      "title": "Apply Manage Privilege",
      "type": "boolean"
    },
    "connection_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionMappingConfig"
      },
      "default": {},
      "description": "Custom mappings for connections",
      "title": "Connection Mapping",
      "type": "object"
    },
    "query_name_template": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "QUERY/{name}",
      "description": "Template for generating dataset urns of consumed queries, the placeholder {query} can be used within the template for inserting the name of the query",
      "title": "Query Name Template"
    },
    "resolve_datasphere_lineage": {
      "default": true,
      "description": "For SAC Live Data Models backed by SAP Datasphere (Data Warehouse Cloud / 'DWC' connections), emit upstream lineage to the backing SAP Datasphere dataset. The Datasphere object's technical name is derived from the SAC model name; the Datasphere space is not exposed by SAC and must be supplied via `connection_mapping.<connection_id>.datasphere_space`. The upstream urn is built as `<space>.<model_name>`. SAC disambiguates repeated links to the same Datasphere object with a trailing numeric suffix that the real object does not carry; when a `datahub_api`/graph connection is available this is corrected by matching the suffix-trimmed name against an ingested Datasphere dataset. Models on connections without a configured `datasphere_space` are skipped with a warning.",
      "title": "Resolve Datasphere Lineage",
      "type": "boolean"
    },
    "resolve_datasphere_column_lineage": {
      "default": true,
      "description": "In addition to table-level DWC lineage, emit column-level lineage to the backing SAP Datasphere dataset. SAC does not expose columns for Live Data Models, so the field list is resolved from the upstream Datasphere dataset's schema in DataHub (requires a `datahub_api`/graph connection) and mirrored onto the SAC dataset, since the live model is a passthrough. Falls back to table-level lineage when the graph or upstream schema is unavailable. No effect unless `resolve_datasphere_lineage` is also enabled.",
      "title": "Resolve Datasphere Column Lineage",
      "type": "boolean"
    }
  },
  "required": [
    "tenant_url",
    "token_url",
    "client_id",
    "client_secret"
  ],
  "title": "SACSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Only models which are used in a Story or an Application will be ingested because there is no dedicated API to retrieve models (only for Stories and Applications).
- SAC does not return a folder for models, so they are grouped under a single synthetic `Models` root (which the connector invents) rather than the meaningless `t` > `4` tree GMS would otherwise derive from the dotted internal dataset name. `folder_pattern` is **not** applied to model browse paths — it is matched only against story and application paths.
- Browse Paths for stories and applications start at SAC's built-in public root folder, which the API returns localized to the resource language (e.g. `Public` / `Öffentlich`). The connector canonicalizes that first segment to a single label (`Public` by default, configurable via `public_root_folder_name`, or `null` to keep the raw localized name) so the same physical root does not split into one browse tree per language. Only a first segment matching `public_root_folder_aliases` (defaults to `Public` / `Öffentlich`, case-insensitive) is rewritten; other shared roots SAC can return (e.g. a team/workspace root such as `Default`) are left unchanged. `folder_pattern` is matched against this canonicalized path.
- Schema metadata is ingested for Import Data Models by default. For acquired (SAC-stored) models it can additionally be ingested via the Data Export Service `$metadata` document by enabling the opt-in `ingest_acquired_data_model_schema_metadata` flag (off by default; requires the "Data Export Service" OAuth grant). Live Data Models (e.g. BW/HANA/DWC) keep their schema in the source system, so it cannot be retrieved from SAC.
- Lineages for Import Data Models cannot be ingested because the API is not providing any information about it.
- SAP BW, SAP HANA, and SAP Datasphere (Data Warehouse Cloud / `DWC` connections) are supported for ingesting the upstream lineages of Live Data Models - a warning is logged for all other connection types, please feel free to open an [issue on GitHub](https://github.com/datahub-project/datahub/issues/new/choose) with the warning message to have this fixed.
- For SAP Datasphere-backed Live Data Models, SAC exposes the underlying object's name but not its Datasphere **space**. Configure the space per connection via `connection_mapping.<connection_id>.datasphere_space` so the upstream `sap-datasphere` dataset urn (`<space>.<model_name>`) can be built. Set `connection_mapping.<connection_id>.convert_urns_to_lowercase` (default `true`) to match the casing used by your SAP Datasphere connector recipe. Models on connections without a configured `datasphere_space` are skipped with a warning (or set `resolve_datasphere_lineage: false` to disable this entirely).
- **Column-level lineage** to SAP Datasphere is emitted when `resolve_datasphere_column_lineage` is enabled (default `true`). SAC does not expose columns for Live Data Models, so the field list is resolved from the upstream SAP Datasphere dataset's schema in DataHub — this requires a `datahub_api`/graph connection and that **SAP Datasphere has already been ingested**. Because the live model is a passthrough, that schema is mirrored onto the SAC dataset and each field is mapped to itself. When the graph or upstream schema is unavailable it falls back to table-level lineage only.
- **Ingest SAP Datasphere before SAC.** SAC disambiguates repeated links to the same Datasphere object with a trailing numeric suffix (e.g. `...STATEMENT2`) that the real object does not carry. The connector corrects this against the graph, but only once the real Datasphere dataset is present. If SAC runs first, the model's upstream edge is written against the as-is (suffixed) name and is corrected on the next SAC run after Datasphere is ingested — `upstreamLineage` is overwritten, so no duplicate edge remains. Column-level lineage has the same ordering requirement.
- Upgrading from a connector version without suffix correction does not clean up `datasetKey` stubs that earlier runs may have emitted for the uncorrected suffixed urns. They are written as non-primary sources, so stale-entity removal leaves them behind as orphan Datasphere nodes; remove them manually (e.g. via `datahub delete`) if they are a concern.
- For some models (e.g., builtin models) it cannot be detected whether the models are Live Data or Import Data Models. Therefore, these models will be ingested only with the `Story` subtype.

#### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Compatibility across tenant generations

The connector reads stories, applications, and models from the `Resources` OData data endpoints (for example `api/v1/Resources`) directly, rather than discovering them from the tenant's `$metadata` document. This keeps ingestion working across SAP Analytics Cloud tenant generations: newer (CAP-based) tenants no longer describe the `Resources` entity set in `$metadata` (it is replaced there by a non-queryable `RESOURCES_INDEX` catalog), but the `Resources` data endpoints remain available and are what the connector uses.


### Code Coordinates
- Class Name: `datahub.ingestion.source.sac.sac.SACSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sac/sac.py)


:::tip Questions?

If you've got any questions on configuring ingestion for SAP Analytics Cloud, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
