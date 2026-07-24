


# BigID

## Overview

[BigID](https://bigid.com/) is a data intelligence platform for data discovery, classification, and privacy. It scans connected data sources, classifies columns and documents against a catalog of classifiers and business glossary terms, and correlates personal data to identities (IDSoR). Learn more in the [official BigID documentation](https://docs.bigid.com/).

The DataHub integration for BigID is an **enrichment** connector: it syncs BigID's classification findings, business glossary terms, and tags onto data assets that already exist in DataHub. It maps BigID business glossary items to GlossaryTerms, classification findings to column-level GlossaryTerms with attribution, BigID tags to DataHub Tags, and BigID risk scores to a structured property. It can optionally create Dataset and schema entities for sources not yet present in DataHub, and supports platform instance mapping, domains, ownership on terms, and stateful ingestion for stale entity removal.

## Concept Mapping

| Source Concept              | DataHub Concept                                                         | Notes                                                                                  |
| --------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Data source (connection)    | [Data Platform](../../metamodel/entities/dataPlatform.md)               | Mapped to a DataHub platform (e.g. `snowflake`, `mysql`) for URN build.                |
| Catalog object              | [Dataset](../../metamodel/entities/dataset.md)                          | Enriched in place; created only when `create_datasets` is enabled.                     |
| Business glossary item      | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID` root [GlossaryNode](../../metamodel/entities/glossaryNode.md). |
| Classification finding      | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md) on SchemaField | Emitted with `MetadataAttribution` recording confidence and counts.                    |
| Unlinked classifier         | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID > Classifier` GlossaryNode when not linked to a term.           |
| IDSoR correlation attribute | [GlossaryTerm](../../metamodel/entities/glossaryTerm.md)                | Grouped under a `BigID > IDSoR` GlossaryNode when not linked to a term.                |
| Tag (OBJECT-scoped)         | [Tag](../../metamodel/entities/tag.md)                                  | Applied to datasets; `hidden` and non-`OBJECT` tags are skipped.                       |
| Risk score                  | Structured Property (`bigid.riskScore`)                                 | Numeric 0–100 value patched onto the dataset.                                          |
| Domain / sub-domain         | [Domain](../../metamodel/entities/domain.md)                            | Optional; controlled by `domain_mode`.                                                 |
| Column profile              | Dataset Profile                                                         | Column-level statistics from BigID `columnProfile` data.                               |


## Module `bigid`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Column-level profiles from BigID columnProfile data. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Stale entity removal via stateful ingestion. Only meaningful with create_datasets=True: in pure enrichment mode the connector owns no Dataset entities, so there is nothing for stale removal to soft-delete (glossary terms, tags and domains are shared, not per-run). |
| [Domains](../../../domains.md) | ✅ | Domain entities created when domain_mode is auto_namespaced or config_map. |
| Extract Ownership | ✅ | Ownership on GlossaryTerms (not Datasets); controlled by owner_type config. |
| Extract Tags | ✅ | BigID tags applied to datasets and columns. |
| Glossary Terms | ✅ | BigID classification findings as GlossaryTerms on SchemaFields. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Platform instance emitted per dataset when platform_instance is configured. |
| Schema Metadata | ✅ | Column schema from BigID columns API (requires create_datasets=True). |
| Table-Level Lineage | ❌ | Not supported. |

### Overview

The `bigid` module ingests classification and governance metadata from BigID into DataHub. It reads BigID's data catalog, business glossary, classifications, and IDSoR correlation results, then enriches matching DataHub datasets with GlossaryTerms, Tags, risk scores, and profiles.

By default this connector runs in **pure enrichment mode** (`create_datasets: false`): it never emits structural aspects and only augments datasets that already exist in DataHub. Enable `create_datasets` to also emit `DatasetProperties` and `SchemaMetadata` for sources that BigID knows about but DataHub does not.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to your BigID instance over HTTPS.
2. **A BigID service account** (a System User) with a long-lived **user token** and read access to the catalog, classification, business glossary, and (if used) correlation/IDSoR APIs. See [Authentication](#authentication) and [Required permissions](#required-permissions) below.
3. **Datasets already present in DataHub** for the sources BigID scans, unless you enable `create_datasets`.
4. **A compatible DataHub version**: **1.8.0+** (DataHub Core) or **2.1.0+** (DataHub Cloud). See [DataHub version compatibility](#datahub-version-compatibility) below.

#### DataHub version compatibility

Field-level enrichment (column classifications and field glossary terms) applies tags and terms to schema fields via PATCH. Adding a field-level tag or term to a field that has no existing `editableSchemaMetadata` entry requires a server-side fix present in **DataHub Core 1.8.0+** and **DataHub Cloud 2.1.0+**. On earlier versions those field-level PATCHes are rejected with `HTTP 422 - fieldPath is required`; dataset-level enrichment is unaffected.

#### Authentication

The connector authenticates to the BigID REST API with a **bearer token**. It does **not** perform an interactive login, so single sign-on (SSO/SAML/OIDC) is never invoked at ingestion time — the token is what grants access. There are two ways to supply that token, in order of preference:

| Config                         | Token kind                                | Lifetime                      | Auto-refresh                                                                                     | Use for                                                                                       |
| ------------------------------ | ----------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `user_token` **(recommended)** | Long-lived user token generated in the UI | Up to 999 days                | Yes — exchanged for a short-lived session token at startup and re-fetched automatically on a 401 | Scheduled / production ingestion                                                              |
| `access_token`                 | Short-lived session token, used directly  | Minutes–hours (BigID default) | No — a run that outlives the token fails                                                         | One-off/manual runs, or SSO-only tenants where you cannot create a service-account user token |

Provide exactly one. If you set **both**, `user_token` takes precedence (it can auto-refresh) and the standalone `access_token` is ignored. Paste the **raw token** for either — do **not** add a `Bearer` prefix (the connector sends it exactly as given).

`user_token` is strongly preferred: it is exchanged for a session token at startup (via `GET /api/v1/refresh-access-token`) and transparently refreshed if that session token expires mid-run, so scheduled ingestion keeps working without manual rotation. `access_token` skips the exchange but is not refreshed, so it is only suitable for short, manual runs.

##### SSO / SAML environments

Because the connector uses token auth rather than an interactive login, an SSO-only tenant does **not** block ingestion — but note:

- **Preferred:** create a local **System User** service account (independent of your SSO directory) and generate a `user_token` for it. This is the most robust option and is unaffected by SSO. Username/password session login (`POST /api/v1/sessions`) is **not** supported by the connector, and would not work for SSO/federated users anyway.
- **If local service users are disallowed:** an SSO user can sign in to BigID and obtain a short-lived session token, then pass it as `access_token` for a manual run. This will expire, so it is not suitable for scheduled ingestion.

##### Generating a user token

Generate the long-lived `user_token` from the BigID UI:

1. Go to **Administration → Access Management** and select (or create) a user from the **System Users List**. A dedicated, read-only service-account user is recommended over a personal login.
2. Open the user's profile in the right-hand detail panel and, in the **Tokens** section, click **Generate**.
3. Set an expiration (BigID allows up to **999 days**) and click **Generate** again.
4. **Copy the token value immediately** — BigID does not display it again after the dialog closes.
5. Click **Save** on the user profile. **This step is required**: an unsaved token stays inactive and the API rejects it with `{"message":"Refresh token not valid"}` (HTTP 401). Tokens cannot be edited after creation — to rotate, generate a new one and Save again.

##### Required permissions

Assign the service user a role with **read** access to the resources the connector reads. It issues only `GET` requests to these endpoints:

| Endpoint                                             | Purpose                                                    | Required when                               |
| ---------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------- |
| `GET /api/v1/refresh-access-token`                   | Exchange the user token for a session token                | Always (when using `user_token`)            |
| `GET /api/v1/ds-connections`                         | Data source → DataHub platform resolution; connection test | Always                                      |
| `GET /api/v1/data-catalog/`                          | Catalog objects (datasets)                                 | Always                                      |
| `GET /api/v1/data-catalog/columns`                   | Column-level schema, classifications, and profiles         | Structured sources                          |
| `GET /api/v1/all-classifications`                    | Classifier → Business Glossary linkage                     | Always                                      |
| `GET /api/v1/business_glossary_items`                | Business Glossary terms                                    | Always                                      |
| `GET /api/v1/data-catalog/results-tuning/attributes` | IDSoR (correlation) attribute → glossary mapping           | Only when `sync_idsor` is enabled (default) |

A read-only role granting the **Data Catalog**, **Classification**, **Business Glossary**, and **Correlation** permission groups covers all of the above. If IDSoR sync is not needed, you can omit the Correlation permission and set `sync_idsor: false`.

#### Connection-to-Platform Resolution

BigID connection `type` values are mapped to DataHub platform names automatically (for example `rdb-postgresql` → `postgres`, `snowflake` → `snowflake`). Two levers let you override this:

- `datasource_platform_mapping` — per-connection overrides of platform, `env`, `platform_instance`, and `convert_urns_to_lowercase`. Required when a connection's type has no built-in mapping, or when a dataset's URN must match a specific platform instance created by a native connector. Set `convert_urns_to_lowercase` on a connection when the native connector's URN casing differs from BigID's default (Snowflake, BigQuery and Redshift are lowercased by default) — for example a Snowflake source ingested with `convert_urns_to_lowercase: false`.
- `connection_pattern` — regex allow/deny patterns matched against the BigID connection name. Use this to scope ingestion to a subset of connections in large deployments that expose hundreds of data sources.

#### Confidence Filtering

Classification findings carry a BigID confidence rank. Ranks map to `HIGH = 0.75`, `MEDIUM = 0.50`, `LOW = 0.25`. Set `minimum_confidence_threshold` (0.0–1.0) to drop low-confidence findings.

#### Domain Handling

BigID `domain`/`sub_domain` values are mapped into DataHub according to `domain_mode`:

- `none` (default) — domain values are stored in `customProperties` only; no domain entities are created.
- `auto_namespaced` — one `urn:li:domain` entity is auto-created per BigID domain/sub-domain, keyed deterministically by name (the human-readable label is carried on `domainProperties.name`).
- `config_map` — BigID domain values are mapped to pre-existing DataHub domain URNs via `domain_mapping`.

In `auto_namespaced` mode the generated domain GUID is **scoped by `env` and `platform_instance`**, mirroring how datasets and data products are separated. The same domain name under different `env` or `platform_instance` values resolves to **distinct** `urn:li:domain` entities. To share a domain across two BigID ingestions, give them the same `env` and `platform_instance`; to keep them separate, vary either. `config_map` mode is unaffected (URNs come from your `domain_mapping`).


### Install the Plugin
```shell
pip install 'acryl-datahub[bigid]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: bigid
  config:
    # Coordinates
    bigid_url: "https://bigid.example.com"

    # Credentials — provide either user_token (recommended) or access_token
    user_token: "${BIGID_USER_TOKEN}"
    # access_token: "${BIGID_ACCESS_TOKEN}"

    # HTTP behaviour (optional)
    # timeout: 60
    # max_retries: 3

    # Environment applied to generated dataset URNs
    env: PROD

    # Scope ingestion to a subset of BigID connections (regex allow/deny)
    # connection_pattern:
    #   allow:
    #     - "^prod-.*"
    #   deny:
    #     - "^sandbox-.*"

    # Per-connection platform / env / instance overrides (and mappings for
    # connection types without a built-in platform mapping)
    # datasource_platform_mapping:
    #   my-snowflake-conn:
    #     platform: snowflake
    #     env: PROD
    #     platform_instance: prod-account

    # Dataset creation (opt-in). Default false = pure enrichment mode.
    # create_datasets: false

    # Classification findings
    # minimum_confidence_threshold: 0.0   # HIGH=0.75, MEDIUM=0.50, LOW=0.25
    # confidence_level_tag: false         # also emit urn:li:tag:bigid.confidence:{LEVEL}

    # Tags
    # sync_tags: true
    # tag_application_types: ["sensitivityClassification", "risk", "userDefined"]

    # Business glossary / classifiers / IDSoR
    # sync_unlinked_classifiers: true
    # sync_idsor: true
    # sync_unstructured_enrichment: false
    # owner_type: user                    # user | group | none
    # domain_mode: none                   # none | auto_namespaced | config_map

    # Stateful ingestion — removes stale entities emitted by this source
    # stateful_ingestion:
    #   enabled: true


sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">bigid_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL of the BigID instance (e.g. 'https://bigid.example.com').  |
| <div className="path-line"><span className="path-main">access_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Short-lived BigID session token, used directly without the startup exchange and NOT auto-refreshed — a run that outlives it fails. Intended for one-off runs or SSO-only tenants where a service-account user_token cannot be created; prefer user_token for scheduled ingestion. Provide either this or user_token; if both are set, user_token is used and this value is ignored. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">confidence_level_tag</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit `urn:li:tag:bigid.confidence:{LEVEL}` alongside each GlossaryTerm on a column. Lossy (can't tie level to a specific term when multiple exist), but visible in DataHub UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">create_datasets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, emit DatasetProperties + SchemaMetadata for datasets not yet in DataHub. Default False (pure enrichment mode — never emits structural aspects). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">domain_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">domain_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "none", "auto_namespaced", "config_map"  |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retries for transient errors. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">minimum_confidence_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Filter column classification findings below this confidence level. Accepts 0.0–1.0 (not a rank string). BigID ranks map to: HIGH = 0.75, MEDIUM = 0.50, LOW = 0.25 (unknown ranks = 0.0). <div className="default-line default-line-with-docs">Default: <span className="default-value">0.0</span></div> |
| <div className="path-line"><span className="path-main">owner_type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "user", "group", "none"  |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">risk_score_structured_property_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | URN of the StructuredProperty used for riskScore values. <div className="default-line default-line-with-docs">Default: <span className="default-value">urn:li:structuredProperty:bigid.riskScore</span></div> |
| <div className="path-line"><span className="path-main">sync_idsor</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit GlossaryTerms for IDSoR (Identity Source of Record) attribute findings from BigID's correlation engine. IDSoR findings are separate from classifier findings and only appear when a Correlation Set is configured and enabled in the scan profile. When the attribute links to an existing Business Glossary term (via glossaryId), that term is reused. Otherwise an auto-generated term is created under a dedicated 'bigid.idsor' GlossaryNode. Term URNs are deterministic GUIDs keyed on the attribute identity. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sync_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit BigID tags as DataHub Tag entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sync_unlinked_classifiers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit GlossaryTerms for classifier findings that have no Business Glossary linkage in BigID. Terms are auto-generated on demand (only when a column finding references the classifier) and placed under the same 'bigid' root GlossaryNode. Term URNs are deterministic GUIDs keyed on the classifier identity. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sync_unstructured_enrichment</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit dataset-level GlossaryTerms and DatasetProfile for unstructured and email sources (SharePoint, Google Drive, O365, Kafka, AI models, etc.) using the attribute_details field returned by BigID's catalog API. Only applies to objects where BigID has classification findings (attribute_details non-empty). Controlled by the same sync_unlinked_classifiers and sync_idsor flags as structured enrichment. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">user_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Recommended auth. Long-lived BigID user token, generated under Administration → Access Management → System Users (Save the user after generating so the token activates). Exchanged for a short-lived session token at startup and auto-refreshed on expiry, so it is safe for scheduled ingestion. Provide the raw token — no 'Bearer' prefix. Provide either this or access_token; if both are set, user_token takes precedence because it can auto-refresh. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">connection_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">dataset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dataset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">datasource_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionPlatformConfig)</span></div> | Per-connection platform override for a single BigID data source.  |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if datasource_platform_mapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub platform name (e.g. 'snowflake', 'mysql').  |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Override dataset-name casing for this connection so BigID's enrichment URN byte-matches the one the native connector emitted. When set, forces (`true`) or disables (`false`) lowercasing of the dataset-name segment. Leave unset to use the built-in per-platform default (Snowflake, BigQuery and Redshift are lowercased). Set `false` for, e.g., a Snowflake connection ingested with `convert_urns_to_lowercase: false`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform instance identifier for this connection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">datasource_platform_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment override for this connection (e.g. 'PROD', 'DEV'). Falls back to top-level env if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">item_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Allow-list of BigID item types to sync. OOTB Personal Data Items are always included regardless of this filter.  |
| <div className="path-line"><span className="path-prefix">item_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">tag_application_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | BigID applicationType values to sync as tags.  |
| <div className="path-line"><span className="path-prefix">tag_application_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
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
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
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
    "ConnectionPlatformConfig": {
      "additionalProperties": false,
      "description": "Per-connection platform override for a single BigID data source.",
      "properties": {
        "platform": {
          "description": "DataHub platform name (e.g. 'snowflake', 'mysql').",
          "title": "Platform",
          "type": "string"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Environment override for this connection (e.g. 'PROD', 'DEV'). Falls back to top-level env if not set.",
          "title": "Env"
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
          "description": "DataHub platform instance identifier for this connection.",
          "title": "Platform Instance"
        },
        "convert_urns_to_lowercase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Override dataset-name casing for this connection so BigID's enrichment URN byte-matches the one the native connector emitted. When set, forces (`true`) or disables (`false`) lowercasing of the dataset-name segment. Leave unset to use the built-in per-platform default (Snowflake, BigQuery and Redshift are lowercased). Set `false` for, e.g., a Snowflake connection ingested with `convert_urns_to_lowercase: false`.",
          "title": "Convert Urns To Lowercase"
        }
      },
      "required": [
        "platform"
      ],
      "title": "ConnectionPlatformConfig",
      "type": "object"
    },
    "DomainMode": {
      "description": "How BigID domain/sub_domain values are mapped into DataHub.",
      "enum": [
        "none",
        "auto_namespaced",
        "config_map"
      ],
      "title": "DomainMode",
      "type": "string"
    },
    "OwnerType": {
      "description": "How BigID owner strings are interpreted when building owner URNs.",
      "enum": [
        "user",
        "group",
        "none"
      ],
      "title": "OwnerType",
      "type": "string"
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
      "default": null
    },
    "bigid_url": {
      "description": "Base URL of the BigID instance (e.g. 'https://bigid.example.com').",
      "title": "Bigid Url",
      "type": "string"
    },
    "user_token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Recommended auth. Long-lived BigID user token, generated under Administration \u2192 Access Management \u2192 System Users (Save the user after generating so the token activates). Exchanged for a short-lived session token at startup and auto-refreshed on expiry, so it is safe for scheduled ingestion. Provide the raw token \u2014 no 'Bearer' prefix. Provide either this or access_token; if both are set, user_token takes precedence because it can auto-refresh.",
      "title": "User Token"
    },
    "access_token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Short-lived BigID session token, used directly without the startup exchange and NOT auto-refreshed \u2014 a run that outlives it fails. Intended for one-off runs or SSO-only tenants where a service-account user_token cannot be created; prefer user_token for scheduled ingestion. Provide either this or user_token; if both are set, user_token is used and this value is ignored.",
      "title": "Access Token"
    },
    "timeout": {
      "default": 60,
      "description": "HTTP request timeout in seconds.",
      "title": "Timeout",
      "type": "integer"
    },
    "max_retries": {
      "default": 3,
      "description": "Maximum number of retries for transient errors.",
      "title": "Max Retries",
      "type": "integer"
    },
    "datasource_platform_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionPlatformConfig"
      },
      "description": "Map BigID connection name \u2192 platform config. Auto-detected from ds-connections API if omitted; explicit entries override.",
      "title": "Datasource Platform Mapping",
      "type": "object"
    },
    "connection_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny patterns matched against the BigID connection (data source) name. Use this to scope ingestion to a subset of connections in large BigID deployments that expose hundreds of data sources. Catalog objects whose source connection is denied are skipped entirely."
    },
    "dataset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex allow/deny patterns matched against the BigID catalog object's fully-qualified name. Complements connection_pattern with finer, dataset-level scoping for large deployments where a single connection exposes many objects. Objects whose fully-qualified name is denied are skipped."
    },
    "create_datasets": {
      "default": false,
      "description": "If True, emit DatasetProperties + SchemaMetadata for datasets not yet in DataHub. Default False (pure enrichment mode \u2014 never emits structural aspects).",
      "title": "Create Datasets",
      "type": "boolean"
    },
    "minimum_confidence_threshold": {
      "default": 0.0,
      "description": "Filter column classification findings below this confidence level. Accepts 0.0\u20131.0 (not a rank string). BigID ranks map to: HIGH = 0.75, MEDIUM = 0.50, LOW = 0.25 (unknown ranks = 0.0).",
      "maximum": 1.0,
      "minimum": 0.0,
      "title": "Minimum Confidence Threshold",
      "type": "number"
    },
    "confidence_level_tag": {
      "default": false,
      "description": "Emit `urn:li:tag:bigid.confidence:{LEVEL}` alongside each GlossaryTerm on a column. Lossy (can't tie level to a specific term when multiple exist), but visible in DataHub UI.",
      "title": "Confidence Level Tag",
      "type": "boolean"
    },
    "item_types": {
      "description": "Allow-list of BigID item types to sync. OOTB Personal Data Items are always included regardless of this filter.",
      "items": {
        "type": "string"
      },
      "title": "Item Types",
      "type": "array"
    },
    "domain_mode": {
      "$ref": "#/$defs/DomainMode",
      "default": "none",
      "description": "Domain handling mode. 'none': store raw domain/sub_domain in customProperties only. 'auto_namespaced': auto-create GUID-based `urn:li:domain` entities (one per BigID domain/sub-domain, keyed deterministically by name). 'config_map': map BigID domain values to existing DataHub domain URNs."
    },
    "domain_mapping": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Used when domain_mode='config_map'. Maps BigID domain string \u2192 DataHub domain URN.",
      "title": "Domain Mapping",
      "type": "object"
    },
    "owner_type": {
      "$ref": "#/$defs/OwnerType",
      "default": "user",
      "description": "How to interpret BigID owner strings. 'user' \u2192 `urn:li:corpuser:{owner}`. 'group' \u2192 `urn:li:corpGroup:{owner}`. 'none' \u2192 stored in customProperties only."
    },
    "sync_tags": {
      "default": true,
      "description": "Emit BigID tags as DataHub Tag entities.",
      "title": "Sync Tags",
      "type": "boolean"
    },
    "tag_application_types": {
      "description": "BigID applicationType values to sync as tags.",
      "items": {
        "type": "string"
      },
      "title": "Tag Application Types",
      "type": "array"
    },
    "risk_score_structured_property_urn": {
      "default": "urn:li:structuredProperty:bigid.riskScore",
      "description": "URN of the StructuredProperty used for riskScore values.",
      "title": "Risk Score Structured Property Urn",
      "type": "string"
    },
    "sync_unlinked_classifiers": {
      "default": true,
      "description": "Emit GlossaryTerms for classifier findings that have no Business Glossary linkage in BigID. Terms are auto-generated on demand (only when a column finding references the classifier) and placed under the same 'bigid' root GlossaryNode. Term URNs are deterministic GUIDs keyed on the classifier identity.",
      "title": "Sync Unlinked Classifiers",
      "type": "boolean"
    },
    "sync_idsor": {
      "default": true,
      "description": "Emit GlossaryTerms for IDSoR (Identity Source of Record) attribute findings from BigID's correlation engine. IDSoR findings are separate from classifier findings and only appear when a Correlation Set is configured and enabled in the scan profile. When the attribute links to an existing Business Glossary term (via glossaryId), that term is reused. Otherwise an auto-generated term is created under a dedicated 'bigid.idsor' GlossaryNode. Term URNs are deterministic GUIDs keyed on the attribute identity.",
      "title": "Sync Idsor",
      "type": "boolean"
    },
    "sync_unstructured_enrichment": {
      "default": false,
      "description": "Emit dataset-level GlossaryTerms and DatasetProfile for unstructured and email sources (SharePoint, Google Drive, O365, Kafka, AI models, etc.) using the attribute_details field returned by BigID's catalog API. Only applies to objects where BigID has classification findings (attribute_details non-empty). Controlled by the same sync_unlinked_classifiers and sync_idsor flags as structured enrichment.",
      "title": "Sync Unstructured Enrichment",
      "type": "boolean"
    }
  },
  "required": [
    "bigid_url"
  ],
  "title": "BigIDSourceConfig",
  "type": "object"
}
```





### Capabilities

- **Business glossary sync** — BigID business glossary items become GlossaryTerms under a `BigID` root GlossaryNode, with domains and ownership optionally attached.
- **Column classification** — classification findings are emitted as GlossaryTerms on schema fields, carrying `MetadataAttribution` that records the classifier, confidence level, and finding counts. Classifiers not linked to a business glossary item are auto-generated under a `BigID > Classifier` node (controlled by `sync_unlinked_classifiers`).
- **IDSoR correlation** — Identity Source of Record findings are resolved via a three-path strategy: reuse a linked business glossary term, auto-generate a term under a `BigID > IDSoR` node, or synthesize one from the raw attribute name.
- **Tags and risk score** — OBJECT-scoped BigID tags become DataHub Tags; risk scores are written to the `bigid.riskScore` structured property.
- **Non-destructive enrichment** — tags, glossary terms, schema-field annotations, and the risk score are applied to existing datasets via PATCH (merge) semantics, so BigID metadata is added alongside — never overwriting — tags and terms that stewards curate in the DataHub UI.
- **No placeholder datasets** — in pure-enrichment mode (`create_datasets: false`) dataset aspects are emitted as non-primary, so BigID never materializes (or later soft-deletes) a dataset that a native connector has not already created. Enable `create_datasets` to have BigID own and create datasets it scans.
- **Profiling** — column-level statistics from BigID `columnProfile` data are emitted as Dataset Profiles.
- **Stateful ingestion** — enables automatic removal of entities emitted by this source when they disappear from BigID.

### Limitations

#### Enrichment adds are not retracted

Because enrichment is applied additively (PATCH), removing a classification, tag, or glossary link **in BigID** does not remove the previously-added term or tag from an existing DataHub dataset on the next run — the PATCH only adds. Stateful ingestion removes entities this connector _owns_ (e.g. glossary terms/nodes it created), but it does not retract annotations merged onto datasets owned by a native connector. Remove such annotations in the DataHub UI if needed.

#### dataPlatformInstance is only emitted when configured

To avoid overwriting the platform instance already set by a native connector (e.g. Snowflake, BigQuery), the `dataPlatformInstance` aspect is emitted only when a `platform_instance` is explicitly configured for the connection.

### Troubleshooting

#### Datasets are not being enriched

The connector matches BigID objects to existing DataHub dataset URNs. If enrichment does not appear, verify that the resolved platform, `env`, `platform_instance`, and URN casing produce a URN that matches the one created by your native connector. Use `datasource_platform_mapping` to align them — including `convert_urns_to_lowercase` when the native connector's casing differs from BigID's per-platform default.

#### No enrichment applied at all

If both the business glossary and classification map fail to load, the connector reports a failure and emits nothing. Check BigID API connectivity and that the token has read access to the catalog, classifications, and glossary APIs.

#### Unknown connection type

When a BigID connection `type` has no built-in platform mapping, the raw type is used as the platform in URNs and a warning is reported. Add an entry to `datasource_platform_mapping` to map it to the correct DataHub platform.


### Code Coordinates
- Class Name: `datahub.ingestion.source.bigid.bigid_source.BigIDSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/bigid/bigid_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for BigID, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
