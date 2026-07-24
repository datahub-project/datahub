


# Open Data Contract Standard

## Overview

The Open Data Contract Standard (ODCS) is the Linux Foundation / Bitol open YAML
standard for data contracts. It defines a vendor-neutral schema covering a contract's
fundamentals, schema, data quality rules, servers, ownership, tags, SLA, support, and
custom properties. Learn more at the audience-friendly [Bitol home page](https://bitol.io/)
and the canonical [open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard)
(spec text and JSON Schemas).

ODCS v3 describes a **producer-published dataset specification** — what a dataset _should_
look like — rather than a bilateral agreement between a specific producer and consumer.
The DataHub `odcs` source therefore models each contract as a **logical dataset** on the
`odcs` platform (a Logical Model), not as a `dataContract` entity. A single ODCS file may
describe multiple tables: each `schema[]` entry becomes its own logical `odcs` Dataset
carrying dataset properties, canonical schema metadata, ownership, tags, a link to the
source document, one Assertion per `quality[]` rule, and one schema-compliance assertion —
all attached to the **logical** dataset. When a `schema[]` entry resolves to a physical
dataset (derived from the contract's typed `servers[]`), the source also links the
physical dataset to the logical one with `logicalParent` (the `PhysicalInstanceOf`
relationship). Propagation of the contract's expectations onto physical instances is
handled by DataHub through that relationship — the source itself never writes assertions
against physical datasets.

These ODCS datasets live on a data platform explicitly marked as logical.

:::info Logical Models are in private beta

Logical Models render in the DataHub UI only when the `LOGICAL_MODELS_ENABLED` feature flag
is on (off by default). The logical `odcs` datasets — and the assertions attached to them —
are ingested either way, but are not displayed until the flag is enabled. See
[Limitations](#limitations) on the module page for the recommended workflow.

:::

## Concept Mapping

The table below lists the ODCS fields covered. Anything not in this table is deliberately
out of scope — see [Limitations](#limitations) on the module page.

| Source Concept                                                                | DataHub Concept                                                                  | Notes                                                                                                                                                             |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id` + `schema[].name`                                                        | Logical `odcs` Dataset URN                                                       | One logical dataset per `schema[]` entry. URN name defaults to `{contract_id}.{schema_name}` (configurable via `logical_dataset_name_template`).                  |
| `name` + `schema[].name`                                                      | `datasetProperties.name`                                                         | Display name `"<contract.name> — <schema.name>"` when both are present.                                                                                           |
| `id`, `version`, `apiVersion`, `status`, `domain`, …                          | `datasetProperties.customProperties` (`odcs.*`)                                  | Provenance keys: `odcs.id`, `odcs.version`, `odcs.apiVersion`, `odcs.status`, `odcs.schemaName`, `odcs.physicalName`, `odcs.sourceFile`, `odcs.qualityRuleCount`. |
| `schema[].description` (fallback: contract `description`)                     | `datasetProperties.description`                                                  | Per-table description wins; `description.purpose` / `usage` / `limitations` objects are concatenated.                                                             |
| `schema[].properties[]` (`name`, `logicalType`, `physicalType`)               | `schemaMetadata.fields[]`                                                        | Canonical schema on the logical dataset. Types map to a `SchemaFieldDataType`; unmapped types fall back to `NullType` and are reported.                           |
| Property `required` / `primaryKey`                                            | `schemaField.nullable` / `isPartOfKey`                                           | `nullable = not required`. Nested `properties[]` use dotted field paths (`address.city`).                                                                         |
| Property `description` / `tags[]`                                             | `schemaField.description` / `globalTags`                                         | `tag_prefix` is prepended to tags if configured.                                                                                                                  |
| Top-level `tags[]`                                                            | `globalTags` on the logical dataset                                              | Emitted when `replicate_contract_metadata=true` (default).                                                                                                        |
| `team[]` entries (`role`, `username`)                                         | `ownership` on the logical dataset                                               | `role` maps to an OwnershipType; usernames/emails map to corpUser owners. Emitted when `replicate_contract_metadata=true`.                                        |
| `authoritativeDefinitions[]` (root, schema, and property level)               | `institutionalMemory`                                                            | Author-provided URLs linked from the logical dataset.                                                                                                             |
| Typed `servers[]` entry                                                       | `logicalParent` on the physical dataset                                          | The `PhysicalInstanceOf` relationship from physical to logical. Platform derives from the spec-required `servers[].type`; names are fully qualified per platform. |
| `schema[].properties[]` (entire declared schema)                              | `SchemaAssertionInfo` (`DATA_SCHEMA` assertion) on the logical dataset           | Pins the contract's schema so drift is an evaluable violation. Compatibility defaults to `SUPERSET` (configurable).                                               |
| v3.1 `metric: nullValues` with `mustBe: 0`                                    | `FieldAssertionInfo` + `FieldValuesAssertion` (`NOT_NULL`)                       | Targets the logical dataset.                                                                                                                                      |
| v3.1 `metric: nullValues` with another threshold                              | `FieldAssertionInfo` + `FieldMetricAssertion` (`NULL_COUNT` / `NULL_PERCENTAGE`) | `unit: percent` selects the percentage metric.                                                                                                                    |
| v3.1 `metric: duplicateValues` / v3.0 `rule: duplicateCount` with `mustBe: 0` | `FieldAssertionInfo` + `FieldMetricAssertion` (`UNIQUE_PERCENTAGE` == 100)       | Zero duplicates ⇔ fully unique column.                                                                                                                            |
| v3.1 `metric: invalidValues` / v3.0 `rule: validValues`                       | `FieldAssertionInfo` + `FieldValuesAssertion` (`IN` or `REGEX_MATCH`)            | Allowed values from `arguments.validValues` (or the v3.0 direct `validValues` list); `arguments.pattern` maps to `REGEX_MATCH`.                                   |
| `metric: rowCount` (v3.0 + v3.1)                                              | `VolumeAssertionInfo`                                                            | `unit: rows` only; percent-based row counts are preserved as custom assertions.                                                                                   |
| `type: sql` with a `query` and a mappable threshold                           | `SqlAssertionInfo`                                                               | The query's metric is compared with the `mustBe*` operator.                                                                                                       |
| All other `quality[]` rules                                                   | `CustomAssertionInfo`                                                            | Original rule intent preserved verbatim as `logic`. See [Quality rule mapping](#quality-rule-mapping).                                                            |


## Module `odcs`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Dataset and column descriptions. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Via standard stateful ingestion (`stateful_ingestion.remove_stale_metadata`): only the logical `odcs` Datasets and Assertions ODCS owns are stale-removed; physical datasets and their `logicalParent` links are never marked removed. |
| Extract Ownership | ✅ | Owners derived from `team[]`. |
| Extract Tags | ✅ | Top-level and column-level `tags`. |
| Schema Metadata | ✅ | Canonical schema (types, descriptions, keys) from `schema[].properties[]`. |

### Overview

The `odcs` module ingests Open Data Contract Standard (ODCS) v3.0 and v3.1 YAML files from
a path, directory, or glob, and models each contract as a **logical dataset** on the `odcs`
platform: dataset properties, canonical schema metadata, ownership, top-level and
column-level tags, a link to the source document, and the contract's `quality[]` rules as
Assertions attached to the logical dataset. When a `schema[]` entry resolves to a physical
dataset (derived from the contract's typed `servers[]`), the source also emits a
`logicalParent` link from the physical dataset to the logical one. ODCS is governed by
the Linux Foundation under the Bitol project; see [bitol.io](https://bitol.io/) and the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).

:::info Looking for the deprecated `*.dhub.dc.yaml` CLI?

The `datahub datacontract upsert` flow that consumes `*.dhub.dc.yaml` files is unrelated to
this source and is being phased out. To ingest ODCS YAML, use `source.type: odcs` via
`datahub ingest -c <recipe.yml>` instead.

:::

### Prerequisites

**A single ODCS file may describe multiple tables.** Each `schema[]` entry becomes its own
logical `odcs` Dataset. Contract-level metadata (description, owners, top-level tags)
applies to every logical dataset by default. See the Concept Mapping table on the platform
Overview above for per-row semantics, and the [Limitations](#limitations) section below for
how to disable replication.

- ODCS v3.0 and v3.1 are supported (any `3.0.x` patch level validates against the same
  v3.0.2 JSON Schema; `3.1.0` against the v3.1 schema). Contracts whose `apiVersion` reports
  v2.x — or any value outside `odcs_versions` — are skipped with a warning.
- **Filter which datasets are ingested with `dataset_pattern`.** The allow/deny regexes
  match the composed logical dataset name (`{contract_id}.{schema_name}`, per
  `logical_dataset_name_template`). A non-matching `schema[]` entry is skipped along with its
  assertions and `logicalParent` link, and recorded under `report.filtered`. Deny takes
  precedence over allow. This is orthogonal to `path` globs, which filter by file location
  rather than contract content.
- **Assertions always attach to the logical dataset.** Quality rules and the
  schema-compliance assertion are emitted whether or not a physical table exists yet, so
  contract-first workflows keep their expectations. Propagation of those expectations onto
  bound physical datasets is handled by DataHub via the `PhysicalInstanceOf` relationship —
  not by this source.
- **Physical binding is derived from the contract itself.** The spec requires
  `servers[].type`; the source maps supported types (postgres, mysql, snowflake, bigquery,
  redshift, databricks, sqlserver, trino) to DataHub platforms and composes fully-qualified
  dataset names from the server's own fields (e.g. `database.schema.table`). Use
  `server_overrides` to refine `env` / `platform_instance`, or `physical_urn_overrides`
  (keyed by contract id, then `schema[]` entry name) for explicit URNs. Binding affects only
  the `logicalParent` link.
- **Derived physical URNs are verified by default.** With a DataHub graph available
  (datahub-rest sink), a derived URN that does not exist in DataHub is left unbound with a
  warning instead of creating a stub dataset (`verify_physical_urns_exist: false` opts out).
  With a file sink there is no graph, and links are emitted without verification.
- **Logical Models are in private beta.** The logical `odcs` datasets this source emits —
  and the assertions attached to them — render in the UI only when `LOGICAL_MODELS_ENABLED`
  is enabled (off by default). The metadata is ingested either way. The recommended workflow
  is: ingest the physical platform source (postgres / snowflake / …), ingest ODCS, and
  enable `LOGICAL_MODELS_ENABLED` to see the logical models, their assertions, and their
  physical links.
- Files are loaded leniently by default (`strict_validation: false`) so that contracts with
  extra or non-conformant fields are accepted with warnings. Spec-valid fields the source
  does not map (SLA, support, pricing, relationships, …) are summarized once per file as an
  info; genuinely unknown fields warn individually. Set `strict_validation: true` to fail on
  JSON Schema violations — recommended for multi-tenant or untrusted directories.
- **Stale-metadata removal uses standard stateful ingestion.** Enable it via the
  `stateful_ingestion` block (server-side checkpoints; requires a DataHub graph). When you
  remove a `schema[]` entry from a contract file, the corresponding logical `odcs` Dataset
  and Assertion URNs are marked removed on the next run. Physical datasets and their
  `logicalParent` links are **never** marked removed — those are owned by their
  platform-of-record source. The `fail_safe_threshold` guard (default 75%) blocks mass
  deletions caused by config or naming changes.
- Symlinks are not followed by default (`follow_symlinks: false`). If your directory layout
  organises contracts using symlinks, set `follow_symlinks: true` deliberately. The default
  is conservative because following symlinks in a shared directory can disclose files
  outside the configured root.
- Files larger than `max_input_file_bytes` (default 5 MB) are skipped with a warning before
  parsing.
- **Contracts can live in object stores, over HTTP, or in a Git repository.** In addition to
  local paths, `path` accepts `s3://` / `gs://` object-store URIs (single file or glob),
  `http(s)://` URLs to a single file, and any mix of these in a list. S3 URIs require an
  `aws_connection` block and GCS URIs a `gcs_connection` block (both validated up front).
  For authenticated `http(s)://` URLs, set an `http_connection` block with either a bearer
  `token` or basic-auth `username`/`password` (the two are mutually exclusive), plus an
  optional `verify_ssl: false` toggle for trusted hosts with self-signed certificates
  (disabling it emits a warning). Public URLs need no `http_connection`.
  Set `git_info` to shallow-clone a repository (using an SSH deploy key) and
  scan it; each non-URI `path` entry is then resolved relative to the checkout (e.g.
  `path: contracts/` or `path: '**/*.odcs.yaml'`). Install the extra dependencies with
  `pip install 'acryl-datahub[odcs]'`.


### Install the Plugin
```shell
pip install 'acryl-datahub[odcs]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: odcs
  config:
    # Path to a single ODCS YAML file, a directory of contracts, or a glob.
    # May also be a list of any of the above. Each file may describe multiple
    # tables (one `schema[]` entry per table) — every entry becomes its own
    # logical `odcs` Dataset carrying the contract's schema and assertions.
    path: ./contracts/

    # `path` also accepts remote locations:
    #   path: s3://my-bucket/contracts/*.odcs.yaml   # requires aws_connection
    #   path: gs://my-bucket/contracts/*.odcs.yaml   # requires gcs_connection
    #   path: https://example.com/orders.odcs.yaml   # single file only (no globs)
    # aws_connection:
    #   aws_region: us-east-1
    # gcs_connection:
    #   credential:
    #     hmac_access_id: "..."
    #     hmac_access_secret: "..."
    # http_connection:                    # auth for http(s):// URLs; omit for public URLs
    #   token: "${ODCS_HTTP_TOKEN}"       # bearer token (Authorization: Bearer ...)
    #   # username: my-user               # OR basic auth (mutually exclusive with token)
    #   # password: "${ODCS_HTTP_PASSWORD}"
    #   # verify_ssl: false               # skip TLS verification (trusted hosts only)

    # Or clone a Git repository and scan it; non-URI `path` entries above are
    # resolved relative to the checkout (e.g. `path: contracts/`).
    # git_info:
    #   repo: https://github.com/acme/data-contracts
    #   branch: main
    #   deploy_key_file: ~/.ssh/datahub_odcs_deploy_key

    # Physical binding (the logicalParent link) is derived automatically from
    # each contract's spec-required `servers[].type` — no mapping is needed for
    # postgres / mysql / snowflake / bigquery / redshift / databricks /
    # sqlserver / trino servers. Optional overrides refine env or
    # platform_instance for a named `servers[].server` value:
    # server_overrides:
    #   - server: prod-snowflake
    #     env: PROD
    #     platform_instance: prod

    # Explicit per-table physical URNs (contract id -> schema entry name -> URN).
    # An empty string deliberately leaves that entry unbound; absent names fall
    # back to server-based derivation.
    # physical_urn_overrides:
    #   "orders.v1":
    #     orders: "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
    #     legacy_orders: ""

    # Standard stateful ingestion: removing a schema[] entry from a contract
    # marks its logical dataset + assertions removed on the next run. Physical
    # datasets are never touched.
    # stateful_ingestion:
    #   enabled: true
    #   remove_stale_metadata: true

    # Optional knobs — uncomment to override defaults.
    #
    # verify_physical_urns_exist: true   # with a DataHub graph: skip logicalParent links whose derived URN doesn't exist
    # lowercase_physical_urns: true      # lowercase composed physical names for platforms that lowercase by default (snowflake)
    # emit_assertions: true              # assertions from quality[] rules, attached to the logical dataset
    # emit_schema_assertion: true        # one DATA_SCHEMA assertion per schema[] entry pinning the contract's schema
    # schema_assertion_compatibility: SUPERSET   # or EXACT_MATCH / SUBSET
    # emit_logical_parent: true          # the physical->logical PhysicalInstanceOf link
    # tag_prefix: "odcs."                # prepend to every emitted tag
    # strict_validation: false           # set true to reject contracts that fail JSON Schema validation
    #
    # Owner identity normalization: ODCS `team[].username` is a username OR an
    # email. Pick at most one of these to match how your identity source keys
    # users. Unresolved owners are reported (report.owners_unresolved) but
    # still emitted — they resolve once the user is provisioned.
    # strip_owner_email_domain: false    # alice@acme.com -> corpuser alice
    # owner_email_domain: acme.com       # alice -> corpuser alice@acme.com
    # odcs_versions: ["3.0.0", "3.0.1", "3.0.2", "3.1.0"]
    # logical_dataset_name_template: "{contract_id}.{schema_name}"  # logical odcs dataset URN name
    #
    # Filter which logical datasets are ingested by their composed name
    # ({contract_id}.{schema_name}). Deny takes precedence over allow.
    # dataset_pattern:
    #   allow:
    #     - ".*"
    #   deny:
    #     - "^sandbox\\..*"
    # follow_symlinks: false             # follow symlinks when scanning a directory
    # file_extensions: [".yaml", ".yml", ".odcs.yaml", ".odcs.yml"]
    # max_input_file_bytes: 5242880      # 5 MB; larger files are skipped with a warning
    #
    # Replicate contract-level ownership/tags to every logical dataset on each
    # ingest. Set false to emit these aspects only at first sight, which
    # preserves manual UI edits but means contract changes don't propagate.
    # replicate_contract_metadata: true

# Default sink is datahub-rest and doesn't need to be configured.
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization.

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">One of string, array</span></div> | Location of ODCS YAML files: a local file, directory, or glob pattern; an `s3://` / `gs://` object-store URI (file or glob); or an `http(s)://` URL to a single file. May also be a list mixing any of the above. When `git_info` is set, non-URI entries are interpreted relative to the repository checkout.  |
| <div className="path-line"><span className="path-prefix">path.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">emit_assertions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit Assertion entities derived from the ODCS `quality[]` rules. Assertions target the logical `odcs` dataset and are emitted whether or not a physical binding resolves. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">emit_logical_parent</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit a `logicalParent` link from each resolved physical dataset to its logical ODCS dataset (the `PhysicalInstanceOf` relationship). Disable to keep ODCS from writing any aspect onto physical datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">emit_schema_assertion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit one DATA_SCHEMA assertion per `schema[]` entry, pinning the contract's declared schema on the logical dataset so schema drift is evaluable as a contract violation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">follow_symlinks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If true, follow symlinks when discovering ODCS files. Off by default to prevent disclosure via symlink escape in shared/multi-tenant directories. When enabled, the source still requires that resolved targets stay within the configured root. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">logical_dataset_name_template</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Template for the logical `odcs` dataset name (the URN name segment). Available placeholders: `{contract_id}`, `{schema_name}`, `{contract_version}`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;contract&#95;id&#125;.&#123;schema&#95;name&#125;</span></div> |
| <div className="path-line"><span className="path-main">lowercase_physical_urns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lowercase composed physical dataset names for platforms whose DataHub connectors lowercase URNs by default (currently snowflake). Set False if your snowflake ingestion runs with convert_urns_to_lowercase disabled. Logical `odcs` dataset URNs always preserve the contract's casing. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_input_file_bytes</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum size of an ODCS YAML file to load. Files exceeding this are skipped with a warning. Defaults to 5 MB. This guards the YAML parser from unbounded inputs. <div className="default-line default-line-with-docs">Default: <span className="default-value">5242880</span></div> |
| <div className="path-line"><span className="path-main">owner_email_domain</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | When set, bare (non-email) team usernames get `@<domain>` appended, so `alice` maps to corpuser `alice@acme.com` — use this when your identity source ingests users by email. Mutually exclusive with `strip_owner_email_domain`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">physical_urn_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,map)</span></div> |   |
| <div className="path-line"><span className="path-main">replicate_contract_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True (default), contract-level Ownership and GlobalTags are written to the logical dataset on every ingest run. Set False to skip emitting contract-level Ownership and GlobalTags so manual UI edits to those aspects survive subsequent ingest runs (the contract is then a one-time enricher rather than a source of truth). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_assertion_compatibility</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "EXACT_MATCH", "SUPERSET", "SUBSET"  |
| <div className="path-line"><span className="path-main">strict_validation</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, ODCS files failing JSON-Schema validation are skipped (logged as warnings). If False (default), schema violations are reported as warnings; the contract is still ingested. Default False matches real-world ODCS files that use deprecated forms (e.g. top-level `quality[]`); set True to fail-fast on schema violations. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">strip_owner_email_domain</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | ODCS team usernames may be emails. When true, the domain is stripped so `alice@acme.com` maps to corpuser `alice` — use this when your identity source (Okta / Azure AD / …) ingests users by bare username. Mutually exclusive with `owner_email_domain`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">tag_prefix</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional prefix prepended to every tag emitted from ODCS `tags` and property-level `tags` (e.g. `odcs.`). Useful for distinguishing ODCS-sourced tags. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">verify_physical_urns_exist</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When a DataHub graph is available (datahub-rest sink), verify each derived physical dataset URN exists before emitting its `logicalParent` link; URNs not found are left unbound with a warning instead of creating stub datasets. With no graph (file sink), emission proceeds without verification. Set False to emit links optimistically for tables that do not exist yet. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_connection</span></div> <div className="type-name-line"><span className="type-name">One of AwsConnectionConfig, null</span></div> | AWS connection details for reading ODCS files from `s3://` URIs in `path`. Required when any `path` entry is an S3 URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_connection.aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dataset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dataset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">file_extensions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | File extensions considered ODCS YAML files when scanning a directory.  |
| <div className="path-line"><span className="path-prefix">file_extensions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">gcs_connection</span></div> <div className="type-name-line"><span className="type-name">One of GCSConnectionConfig, null</span></div> | GCS connection (HMAC credentials via the S3-compatible API) for reading ODCS files from `gs://` URIs in `path`. Required when any `path` entry is a GCS URI. See https://cloud.google.com/storage/docs/authentication/hmackeys <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">gcs_connection.</span><span className="path-main">credential</span>&nbsp;<abbr title="Required if gcs_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">HMACKey</span></div> |   |
| <div className="path-line"><span className="path-prefix">gcs_connection.credential.</span><span className="path-main">hmac_access_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Access ID  |
| <div className="path-line"><span className="path-prefix">gcs_connection.credential.</span><span className="path-main">hmac_access_secret</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Secret  |
| <div className="path-line"><span className="path-prefix">gcs_connection.</span><span className="path-main">endpoint_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | GCS S3-compatible endpoint URL. Useful for testing with local S3-compatible servers. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://storage.googleapis.com</span></div> |
| <div className="path-line"><span className="path-main">git_info</span></div> <div className="type-name-line"><span className="type-name">One of GitInfo, null</span></div> | Git repository to shallow-clone and scan for ODCS files, authenticated with an SSH deploy key. When set, each non-URI `path` entry is resolved relative to the repository checkout (e.g. `path: contracts/` or `path: '**/*.odcs.yaml'`). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if git_info is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.  |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">clone_timeout</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Timeout in seconds for git clone operations. Set to None to disable the timeout. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">One of string(file-path), null</span></div> | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_subdir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">http_connection</span></div> <div className="type-name-line"><span className="type-name">One of HTTPConnectionConfig, null</span></div> | Authentication and TLS options for reading ODCS files from `http(s)://` URLs in `path`. Supports a bearer token or HTTP basic auth, and a `verify_ssl` toggle. Omit for public URLs that need no authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">http_connection.</span><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for HTTP Basic authentication (requires username). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">http_connection.</span><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Bearer token sent as an `Authorization: Bearer <token>` header. Mutually exclusive with username/password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">http_connection.</span><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Username for HTTP Basic authentication (requires password). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">http_connection.</span><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Verify the server's TLS certificate. Disable only for trusted hosts with self-signed certificates. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">odcs_versions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of supported ODCS `apiVersion` values. Contracts with `apiVersion` outside this list are skipped with a warning.  |
| <div className="path-line"><span className="path-prefix">odcs_versions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">server_overrides</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Optional per-server overrides for physical binding. The physical platform is derived from the contract's spec-required `servers[].type`; an override refines env / platform_instance / platform for a named `servers[].server` value. Binding only affects the `logicalParent` link — assertions always attach to the logical dataset.  |
| <div className="path-line"><span className="path-prefix">server_overrides.</span><span className="path-main">ServerMapping</span></div> <div className="type-name-line"><span className="type-name">ServerMapping</span></div> | Per-server overrides for physical dataset binding. <br />  <br /> The physical platform is normally derived from the spec-required <br /> `servers[].type`; an override entry refines that derivation for a named <br /> `servers[].server` — most commonly to set `env` or `platform_instance`, <br /> or to force a different platform id.  |
| <div className="path-line"><span className="path-prefix">server_overrides.ServerMapping.</span><span className="path-main">server</span>&nbsp;<abbr title="Required if ServerMapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The value of `servers[].server` in the ODCS contract. If `match_any` is True, this is treated as a wildcard match for any server.  |
| <div className="path-line"><span className="path-prefix">server_overrides.ServerMapping.</span><span className="path-main">match_any</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, this mapping matches any `servers[].server` not matched by a more specific mapping. Use a single catch-all mapping to apply one override to all contracts. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">server_overrides.ServerMapping.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional DataHub data platform identifier (e.g. `postgres`, `snowflake`). When unset, the platform is derived from the server's `type`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_overrides.ServerMapping.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional DataHub platform instance, used when the same platform is deployed multiple times (e.g. multiple Snowflake accounts). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_overrides.ServerMapping.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment / fabric type for the produced physical dataset URNs (e.g. PROD, DEV). <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "AwsAssumeRoleConfig": {
      "additionalProperties": true,
      "properties": {
        "RoleArn": {
          "description": "ARN of the role to assume.",
          "title": "Rolearn",
          "type": "string"
        },
        "ExternalId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "External ID to use when assuming the role.",
          "title": "Externalid"
        }
      },
      "required": [
        "RoleArn"
      ],
      "title": "AwsAssumeRoleConfig",
      "type": "object"
    },
    "AwsConnectionConfig": {
      "additionalProperties": false,
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "properties": {
        "aws_access_key_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Access Key Id"
        },
        "aws_secret_access_key": {
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
          "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Secret Access Key"
        },
        "aws_session_token": {
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
          "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Session Token"
        },
        "aws_role": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "items": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "$ref": "#/$defs/AwsAssumeRoleConfig"
                  }
                ]
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
          "title": "Aws Role"
        },
        "aws_profile": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
          "title": "Aws Profile"
        },
        "aws_region": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS region code.",
          "title": "Aws Region"
        },
        "aws_endpoint_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
          "title": "Aws Endpoint Url"
        },
        "aws_proxy": {
          "anyOf": [
            {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
          "title": "Aws Proxy"
        },
        "aws_retry_num": {
          "default": 5,
          "description": "Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "title": "Aws Retry Num",
          "type": "integer"
        },
        "aws_retry_mode": {
          "default": "standard",
          "description": "Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "enum": [
            "legacy",
            "standard",
            "adaptive"
          ],
          "title": "Aws Retry Mode",
          "type": "string"
        },
        "read_timeout": {
          "default": 60,
          "description": "The timeout for reading from the connection (in seconds).",
          "title": "Read Timeout",
          "type": "number"
        },
        "aws_advanced_config": {
          "additionalProperties": true,
          "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
          "title": "Aws Advanced Config",
          "type": "object"
        }
      },
      "title": "AwsConnectionConfig",
      "type": "object"
    },
    "GCSConnectionConfig": {
      "additionalProperties": false,
      "description": "GCS connection using HMAC keys, accessed via the S3-compatible XML API.",
      "properties": {
        "credential": {
          "$ref": "#/$defs/HMACKey",
          "description": "GCS HMAC credentials. See https://cloud.google.com/storage/docs/authentication/hmackeys"
        },
        "endpoint_url": {
          "default": "https://storage.googleapis.com",
          "description": "GCS S3-compatible endpoint URL. Useful for testing with local S3-compatible servers.",
          "title": "Endpoint Url",
          "type": "string"
        }
      },
      "required": [
        "credential"
      ],
      "title": "GCSConnectionConfig",
      "type": "object"
    },
    "GitInfo": {
      "additionalProperties": false,
      "description": "A reference to a Git repository, including a deploy key that can be used to clone it.",
      "properties": {
        "repo": {
          "description": "Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.",
          "title": "Repo",
          "type": "string"
        },
        "branch": {
          "default": "main",
          "description": "Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
          "title": "Branch",
          "type": "string"
        },
        "url_subdir": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations.",
          "title": "Url Subdir"
        },
        "url_template": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}",
          "title": "Url Template"
        },
        "deploy_key_file": {
          "anyOf": [
            {
              "format": "file-path",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase.",
          "title": "Deploy Key File"
        },
        "deploy_key": {
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
          "description": "A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.",
          "title": "Deploy Key"
        },
        "repo_ssh_locator": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.",
          "title": "Repo Ssh Locator"
        },
        "clone_timeout": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 300,
          "description": "Timeout in seconds for git clone operations. Set to None to disable the timeout.",
          "title": "Clone Timeout"
        }
      },
      "required": [
        "repo"
      ],
      "title": "GitInfo",
      "type": "object"
    },
    "HMACKey": {
      "additionalProperties": false,
      "properties": {
        "hmac_access_id": {
          "description": "Access ID",
          "title": "Hmac Access Id",
          "type": "string"
        },
        "hmac_access_secret": {
          "description": "Secret",
          "format": "password",
          "title": "Hmac Access Secret",
          "type": "string",
          "writeOnly": true
        }
      },
      "required": [
        "hmac_access_id",
        "hmac_access_secret"
      ],
      "title": "HMACKey",
      "type": "object"
    },
    "HTTPConnectionConfig": {
      "additionalProperties": false,
      "description": "Authentication and TLS options for reading files over http(s)://.",
      "properties": {
        "token": {
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
          "description": "Bearer token sent as an `Authorization: Bearer <token>` header. Mutually exclusive with username/password.",
          "title": "Token"
        },
        "username": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Username for HTTP Basic authentication (requires password).",
          "title": "Username"
        },
        "password": {
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
          "description": "Password for HTTP Basic authentication (requires username).",
          "title": "Password"
        },
        "verify_ssl": {
          "default": true,
          "description": "Verify the server's TLS certificate. Disable only for trusted hosts with self-signed certificates.",
          "title": "Verify Ssl",
          "type": "boolean"
        }
      },
      "title": "HTTPConnectionConfig",
      "type": "object"
    },
    "SchemaAssertionCompatibility": {
      "description": "Compatibility mode for the emitted DATA_SCHEMA assertion.\n\nMember names are the DataHub `SchemaAssertionCompatibilityClass` constants,\nso this enum is the single source of truth for the valid set; the mapper\npasses the member value straight onto the assertion aspect.",
      "enum": [
        "EXACT_MATCH",
        "SUPERSET",
        "SUBSET"
      ],
      "title": "SchemaAssertionCompatibility",
      "type": "string"
    },
    "ServerMapping": {
      "additionalProperties": false,
      "description": "Per-server overrides for physical dataset binding.\n\nThe physical platform is normally derived from the spec-required\n`servers[].type`; an override entry refines that derivation for a named\n`servers[].server` \u2014 most commonly to set `env` or `platform_instance`,\nor to force a different platform id.",
      "properties": {
        "server": {
          "description": "The value of `servers[].server` in the ODCS contract. If `match_any` is True, this is treated as a wildcard match for any server.",
          "title": "Server",
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
          "description": "Optional DataHub data platform identifier (e.g. `postgres`, `snowflake`). When unset, the platform is derived from the server's `type`.",
          "title": "Platform"
        },
        "env": {
          "default": "PROD",
          "description": "The environment / fabric type for the produced physical dataset URNs (e.g. PROD, DEV).",
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
          "description": "Optional DataHub platform instance, used when the same platform is deployed multiple times (e.g. multiple Snowflake accounts).",
          "title": "Platform Instance"
        },
        "match_any": {
          "default": false,
          "description": "If True, this mapping matches any `servers[].server` not matched by a more specific mapping. Use a single catch-all mapping to apply one override to all contracts.",
          "title": "Match Any",
          "type": "boolean"
        }
      },
      "required": [
        "server"
      ],
      "title": "ServerMapping",
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
  "description": "Config for the ODCS ingestion source.\n\nReads Open Data Contract Standard (ODCS) v3.x YAML files from a path (file,\ndirectory, or glob) and materializes each `schema[]` entry as a **logical\ndataset** on the `odcs` platform: dataset properties, canonical schema\nmetadata, ownership, tags, quality + schema-compliance assertions, and a\nlink to the source document. When a physical dataset can be resolved from\nthe contract's typed `servers[]` (refined via `server_overrides` or\n`physical_urn_overrides`), the source also emits a `logicalParent` link\nfrom the physical dataset to the logical one.",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
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
      "description": "Stateful Ingestion Config"
    },
    "path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      ],
      "description": "Location of ODCS YAML files: a local file, directory, or glob pattern; an `s3://` / `gs://` object-store URI (file or glob); or an `http(s)://` URL to a single file. May also be a list mixing any of the above. When `git_info` is set, non-URI entries are interpreted relative to the repository checkout.",
      "title": "Path"
    },
    "aws_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/AwsConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS connection details for reading ODCS files from `s3://` URIs in `path`. Required when any `path` entry is an S3 URI."
    },
    "gcs_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/GCSConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "GCS connection (HMAC credentials via the S3-compatible API) for reading ODCS files from `gs://` URIs in `path`. Required when any `path` entry is a GCS URI. See https://cloud.google.com/storage/docs/authentication/hmackeys"
    },
    "git_info": {
      "anyOf": [
        {
          "$ref": "#/$defs/GitInfo"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Git repository to shallow-clone and scan for ODCS files, authenticated with an SSH deploy key. When set, each non-URI `path` entry is resolved relative to the repository checkout (e.g. `path: contracts/` or `path: '**/*.odcs.yaml'`)."
    },
    "http_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/HTTPConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Authentication and TLS options for reading ODCS files from `http(s)://` URLs in `path`. Supports a bearer token or HTTP basic auth, and a `verify_ssl` toggle. Omit for public URLs that need no authentication."
    },
    "server_overrides": {
      "description": "Optional per-server overrides for physical binding. The physical platform is derived from the contract's spec-required `servers[].type`; an override refines env / platform_instance / platform for a named `servers[].server` value. Binding only affects the `logicalParent` link \u2014 assertions always attach to the logical dataset.",
      "items": {
        "$ref": "#/$defs/ServerMapping"
      },
      "title": "Server Overrides",
      "type": "array"
    },
    "lowercase_physical_urns": {
      "default": true,
      "description": "Lowercase composed physical dataset names for platforms whose DataHub connectors lowercase URNs by default (currently snowflake). Set False if your snowflake ingestion runs with convert_urns_to_lowercase disabled. Logical `odcs` dataset URNs always preserve the contract's casing.",
      "title": "Lowercase Physical Urns",
      "type": "boolean"
    },
    "verify_physical_urns_exist": {
      "default": true,
      "description": "When a DataHub graph is available (datahub-rest sink), verify each derived physical dataset URN exists before emitting its `logicalParent` link; URNs not found are left unbound with a warning instead of creating stub datasets. With no graph (file sink), emission proceeds without verification. Set False to emit links optimistically for tables that do not exist yet.",
      "title": "Verify Physical Urns Exist",
      "type": "boolean"
    },
    "tag_prefix": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional prefix prepended to every tag emitted from ODCS `tags` and property-level `tags` (e.g. `odcs.`). Useful for distinguishing ODCS-sourced tags.",
      "title": "Tag Prefix"
    },
    "strip_owner_email_domain": {
      "default": false,
      "description": "ODCS team usernames may be emails. When true, the domain is stripped so `alice@acme.com` maps to corpuser `alice` \u2014 use this when your identity source (Okta / Azure AD / \u2026) ingests users by bare username. Mutually exclusive with `owner_email_domain`.",
      "title": "Strip Owner Email Domain",
      "type": "boolean"
    },
    "owner_email_domain": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "When set, bare (non-email) team usernames get `@<domain>` appended, so `alice` maps to corpuser `alice@acme.com` \u2014 use this when your identity source ingests users by email. Mutually exclusive with `strip_owner_email_domain`.",
      "title": "Owner Email Domain"
    },
    "strict_validation": {
      "default": false,
      "description": "If True, ODCS files failing JSON-Schema validation are skipped (logged as warnings). If False (default), schema violations are reported as warnings; the contract is still ingested. Default False matches real-world ODCS files that use deprecated forms (e.g. top-level `quality[]`); set True to fail-fast on schema violations.",
      "title": "Strict Validation",
      "type": "boolean"
    },
    "replicate_contract_metadata": {
      "default": true,
      "description": "If True (default), contract-level Ownership and GlobalTags are written to the logical dataset on every ingest run. Set False to skip emitting contract-level Ownership and GlobalTags so manual UI edits to those aspects survive subsequent ingest runs (the contract is then a one-time enricher rather than a source of truth).",
      "title": "Replicate Contract Metadata",
      "type": "boolean"
    },
    "odcs_versions": {
      "description": "List of supported ODCS `apiVersion` values. Contracts with `apiVersion` outside this list are skipped with a warning.",
      "items": {
        "type": "string"
      },
      "title": "Odcs Versions",
      "type": "array"
    },
    "emit_assertions": {
      "default": true,
      "description": "Whether to emit Assertion entities derived from the ODCS `quality[]` rules. Assertions target the logical `odcs` dataset and are emitted whether or not a physical binding resolves.",
      "title": "Emit Assertions",
      "type": "boolean"
    },
    "emit_schema_assertion": {
      "default": true,
      "description": "Whether to emit one DATA_SCHEMA assertion per `schema[]` entry, pinning the contract's declared schema on the logical dataset so schema drift is evaluable as a contract violation.",
      "title": "Emit Schema Assertion",
      "type": "boolean"
    },
    "schema_assertion_compatibility": {
      "$ref": "#/$defs/SchemaAssertionCompatibility",
      "default": "SUPERSET",
      "description": "Compatibility mode for the DATA_SCHEMA assertion: `SUPERSET` (an instance must contain at least the contract's fields; extras allowed), `EXACT_MATCH`, or `SUBSET`."
    },
    "emit_logical_parent": {
      "default": true,
      "description": "Whether to emit a `logicalParent` link from each resolved physical dataset to its logical ODCS dataset (the `PhysicalInstanceOf` relationship). Disable to keep ODCS from writing any aspect onto physical datasets.",
      "title": "Emit Logical Parent",
      "type": "boolean"
    },
    "physical_urn_overrides": {
      "additionalProperties": {
        "additionalProperties": {
          "type": "string"
        },
        "type": "object"
      },
      "description": "Map of ODCS contract `id` to a map of `schema[]` entry NAME to an explicit physical DataHub Dataset URN, bypassing server-based derivation for those entries. An empty-string URN deliberately leaves the named entry unbound; schema entries absent from the map fall back to server-based derivation. Keys that match no schema entry produce a warning (typo guard).",
      "title": "Physical Urn Overrides",
      "type": "object"
    },
    "logical_dataset_name_template": {
      "default": "{contract_id}.{schema_name}",
      "description": "Template for the logical `odcs` dataset name (the URN name segment). Available placeholders: `{contract_id}`, `{schema_name}`, `{contract_version}`.",
      "title": "Logical Dataset Name Template",
      "type": "string"
    },
    "dataset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex allow/deny patterns applied to the logical `odcs` dataset name (as composed by `logical_dataset_name_template`, e.g. `{contract_id}.{schema_name}`). Schema entries whose logical name does not match are skipped along with their assertions and `logicalParent` link. Matched case-insensitively by default."
    },
    "file_extensions": {
      "description": "File extensions considered ODCS YAML files when scanning a directory.",
      "items": {
        "type": "string"
      },
      "title": "File Extensions",
      "type": "array"
    },
    "max_input_file_bytes": {
      "default": 5242880,
      "description": "Maximum size of an ODCS YAML file to load. Files exceeding this are skipped with a warning. Defaults to 5 MB. This guards the YAML parser from unbounded inputs.",
      "title": "Max Input File Bytes",
      "type": "integer"
    },
    "follow_symlinks": {
      "default": false,
      "description": "If true, follow symlinks when discovering ODCS files. Off by default to prevent disclosure via symlink escape in shared/multi-tenant directories. When enabled, the source still requires that resolved targets stay within the configured root.",
      "title": "Follow Symlinks",
      "type": "boolean"
    }
  },
  "required": [
    "path"
  ],
  "title": "ODCSSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for which DataHub
features this source emits. The notes below cover ODCS-specific behavior that the table
does not capture.

#### Quality rule mapping

Each entry in a `schema[]` (table-level) or `properties[]` (column-level) `quality[]` array
becomes a DataHub assertion **attached to the logical `odcs` dataset**, emitted whether or
not a physical binding resolves. The library vocabulary is spec-exact and
version-dependent:

| ODCS rule                                                                         | DataHub aspect                                                                   |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| v3.1 `metric: nullValues` with `mustBe: 0`                                        | `FieldAssertionInfo` + `FieldValuesAssertion` (`NOT_NULL`)                       |
| v3.1 `metric: nullValues` with another threshold                                  | `FieldAssertionInfo` + `FieldMetricAssertion` (`NULL_COUNT` / `NULL_PERCENTAGE`) |
| v3.1 `metric: duplicateValues` / v3.0 `rule: duplicateCount` with `mustBe: 0`     | `FieldAssertionInfo` + `FieldMetricAssertion` (`UNIQUE_PERCENTAGE` == 100)       |
| v3.1 `metric: invalidValues` (`arguments.validValues`) / v3.0 `rule: validValues` | `FieldAssertionInfo` + `FieldValuesAssertion` (`IN`)                             |
| v3.1 `metric: invalidValues` with `arguments.pattern`                             | `FieldAssertionInfo` + `FieldValuesAssertion` (`REGEX_MATCH`)                    |
| `metric: rowCount` (v3.0 `rule: rowCount`)                                        | `VolumeAssertionInfo` (`unit: rows` only)                                        |
| `type: sql` with a `query` and a mappable threshold                               | `SqlAssertionInfo`                                                               |
| `type: custom` (`engine` + `implementation`)                                      | `CustomAssertionInfo` (`type` = the engine, `logic` = the implementation)        |
| `type: text`                                                                      | `CustomAssertionInfo` (`logic` = the description)                                |
| Anything else with content                                                        | `CustomAssertionInfo` (original rule intent preserved as `logic`)                |
| Anything with no operator and no body                                             | Skipped with a warning                                                           |

The v3.1 library key is `metric`; `rule` is accepted as the v3.0 canonical key and as the
deprecated v3.1 alias (v3.1 documents using `rule` get an informational notice). Every
assertion carries ODCS provenance in `customProperties` (`odcs.id`, `odcs.rule.id`,
`odcs.rule.metric`, `odcs.rule.unit`, serialized `odcs.rule.arguments`, dimension /
severity / businessImpact when present), a `dataPlatformInstance` aspect attributing it to
the `odcs` platform, and an `externalUrl` from the rule's `authoritativeDefinitions` when
provided. Assertion URNs are seeded from the spec's `quality.id` when present, so renames
and reordering do not churn identities.

:::warning Routing is exact; nothing is approximated

Tolerances map to typed assertions only when they are exactly representable:

- No threshold at all (the v3.0 `validValues` form) and `mustBe: 0` both mean "no failing
  rows tolerated".
- An integral `mustBeLessOrEqualTo` maps to a fail threshold in rows (`unit: rows`) or
  percent (`unit: percent`).
- Everything else — strict less-than tolerances, non-integral percents,
  `mustNotBeBetween`, duplicate-count tolerances, percent-based `rowCount`, multi-column
  `duplicateValues` (`arguments.properties`), `missingValues` — is preserved as a
  `CustomAssertionInfo` whose `logic` carries a stable rendition of the original rule
  (metric, arguments, thresholds, unit) rather than being approximated into a typed shape.
- Rules routed to custom are listed in `report.rules_routed_to_custom`; rules with no
  modelable content at all are skipped and listed in `report.rules_skipped_no_threshold`.

:::

#### Schema-compliance assertion

For every `schema[]` entry that declares properties, the source emits one `DATA_SCHEMA`
assertion (`SchemaAssertionInfo`) on the logical dataset carrying the contract's declared
schema. This makes schema drift an evaluable contract violation rather than an implied
one. `schema_assertion_compatibility` controls the mode: `SUPERSET` (default — an instance
must contain at least the contract's fields, extras allowed), `EXACT_MATCH`, or `SUBSET`.
Disable with `emit_schema_assertion: false`.

#### Physical dataset binding (the `logicalParent` link)

Binding exists to link physical datasets to their logical model — assertions never depend
on it. Resolution per `schema[]` entry, in priority order:

1. `physical_urn_overrides[<contract id>][<schema entry name>]` — an explicit URN, or an
   empty string to deliberately leave the entry unbound. Entries absent from the map fall
   back to derivation; keys that match no schema entry warn.
2. The contract's first mappable `servers[]` entry. The platform comes from the
   spec-required `servers[].type` (or a matching `server_overrides` entry), and the table
   name (`physicalName`, falling back to `name`) is qualified with the server's own fields
   per the platform's URN convention:

| Server `type` | DataHub platform | Physical name                                                                              |
| ------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `postgres`    | `postgres`       | `database.schema.table`                                                                    |
| `redshift`    | `redshift`       | `database.schema.table`                                                                    |
| `sqlserver`   | `mssql`          | `database.schema.table`                                                                    |
| `snowflake`   | `snowflake`      | `database.schema.table` (lowercased by default; `lowercase_physical_urns: false` opts out) |
| `bigquery`    | `bigquery`       | `project.dataset.table`                                                                    |
| `databricks`  | `databricks`     | `catalog.schema.table`                                                                     |
| `trino`       | `trino`          | `catalog.schema.table`                                                                     |
| `mysql`       | `mysql`          | `database.table`                                                                           |
| `oracle`      | `oracle`         | not composable — supply a dotted `physicalName` or an explicit override                    |
| anything else | —                | unbound (logical dataset and assertions unaffected)                                        |

A `physicalName` that already contains a dot is used verbatim (assumed pre-qualified) and
counted in `report.physical_names_passthrough`. Missing server fields leave the entry
unbound with an actionable reason — the source never guesses a schema name. When a DataHub
graph is available and `verify_physical_urns_exist` is on (default), derived URNs that do
not exist in DataHub are left unbound with a warning instead of creating stub datasets.
Two schema entries binding the same physical dataset warn: `logicalParent` is
single-valued, so the last writer wins.

### Limitations

- ODCS v3.0 and v3.1 only. Contracts reporting v2.x in `apiVersion` are skipped with a
  warning.
- **Logical Models are in private beta** and render in the UI only when
  `LOGICAL_MODELS_ENABLED` is enabled (off by default). The logical datasets and their
  assertions are ingested while the flag is off — they just aren't displayed.
- **Nested-column assertions and field-path resolution.** Field-scoped assertions reference
  the column by its dotted property path (`address.city`), matching the logical dataset's
  own `schemaMetadata`. Propagation onto physical datasets whose connectors encode nested
  struct paths differently is subject to the platform propagation mechanism.
- **Property-level `unique: true` / `required: true` flags are not emitted as
  assertions** — they map into `schemaMetadata` (nullability, keys) and are enforced via
  the schema-compliance assertion; express uniqueness checks as `quality[]` rules
  (`metric: duplicateValues` with `mustBe: 0`) to get a typed field assertion.
- File loading is capped at 5 MB by default (`max_input_file_bytes`). Larger YAML files
  are skipped with a warning before parsing.
- Out of scope: SLA (`slaProperties`), `support`, `price`, v3.1 `relationships` (foreign
  keys), `customProperties` → DataHub `customProperties` (only the `odcs.*` provenance
  subset is emitted), `classification` → `GlossaryTerm` linking, schemaField-level
  `logicalParent` column links, and ODCS export. Spec-valid-but-unmapped fields are
  reported once per file via `report.spec_fields_ignored`. These may land in a follow-up.
- **Data products / output ports (ODPS) are not modeled.** The contract-level
  `dataProduct` field is emitted only as the `odcs.dataProduct` custom property — it is
  **not** linked to a DataHub `DataProduct` entity, and ODPS output ports are not read at
  all. "One contract across several output ports" and "several contracts on one port" are
  therefore not representable today. Dataset-level contracts (via the logical dataset and
  its `logicalParent` link) are the supported unit.
- **Schema validation depends on the bundled JSON Schemas.** The v3.0.2 / v3.1.0 schemas
  are vendored with the plugin, so `strict_validation` normally works out of the box. If a
  contract declares a supported `apiVersion` for which no validator is available (e.g. a
  packaging regression that dropped a schema file), the contract is parsed **without**
  schema checking rather than being rejected — `strict_validation` becomes a no-op for
  those contracts. Confirm the schemas shipped with your install if you rely on strict
  validation as a hard gate.
- **Contract metadata replication**: By default, contract-level ownership and tags are
  written to every logical dataset on each run. If you edit these aspects in the DataHub UI,
  they will be overwritten on the next ingest. Set `replicate_contract_metadata: false` to
  disable replication (useful for one-time enrichment workflows).
- **Mixed-platform `servers[]`**: binding uses the first server whose `type` maps to a
  platform; contracts that reference multiple platforms bind to that one only. Use
  `physical_urn_overrides` for per-table control.

### Troubleshooting

#### Where do the emitted assertions appear?

On the **logical `odcs` dataset** — open it in the UI (requires `LOGICAL_MODELS_ENABLED`)
and look under its **Quality / Assertions** tab. Assertions are not written to physical
datasets by this source; DataHub propagates expectations to physical instances via the
`PhysicalInstanceOf` relationship.

#### My contract produced no logicalParent link

Check `report.unmappable_servers` and the per-entry info messages: the contract may
declare no `servers[]`, use a server `type` with no platform mapping (e.g. `kafka`, `s3`),
or be missing the fields needed to qualify a table name (e.g. a postgres server without
`schema`). With a DataHub graph attached, `report.physical_urns_unverified` counts derived
URNs that were skipped because they do not exist in DataHub yet — ingest the physical
platform first, or set `verify_physical_urns_exist: false` to link optimistically.

#### My logical `odcs` dataset doesn't appear in the UI

Logical Models are in private beta and require the `LOGICAL_MODELS_ENABLED` feature flag
(off by default). Enable it to view logical datasets, their assertions, and their
`logicalParent` links. The metadata is still ingested while the flag is off — it just
isn't displayed. The flag is set on the GMS service (for self-hosted OSS, the
`LOGICAL_MODELS_ENABLED` environment variable on the `datahub-gms` container; on DataHub
Cloud, ask your administrator to enable it).

#### My `missingValues`/`mustNotBeBetween`/vendor rule shows up as a Custom Assertion

Expected. See [Quality rule mapping](#quality-rule-mapping) for the exact typed-assertion
allowlist and the threshold-representability rules. Anything outside them is preserved
verbatim as `CustomAssertionInfo` rather than approximated.

#### I removed a table from my ODCS file but the physical dataset is still in DataHub

That's expected. ODCS owns the logical `odcs` Dataset and the Assertions it emitted; the
physical Dataset belongs to its platform-of-record source (postgres / snowflake / …), and
the `logicalParent` link is a non-destructive enrichment. With `stateful_ingestion`
enabled, removing the `schema[]` entry marks the logical dataset and its assertions
removed on the next ODCS ingest — never the physical dataset.

Note the asymmetry: because the physical dataset is intentionally kept out of the ODCS
stateful checkpoint, the `logicalParent` pointer previously written onto it is **not**
cleared when its logical parent is soft-deleted. The physical dataset therefore retains a
`logicalParent` pointing at a now soft-deleted logical dataset until it is next bound or
manually cleaned up. This is deliberate (ODCS must never mutate a physical dataset it does
not own), but it does leave a stale pointer.

#### My contract owner shows up as an unresolved user

ODCS `team[].username` is a username **or** an email, and DataHub resolves the owner
against whatever identifier your identity source (Okta / Azure AD / SCIM / …) ingests
users under. If the two disagree — the contract says `alice@acme.com` but DataHub knows
the user as `alice`, or vice versa — the ownership reference dangles. Set exactly one of
`strip_owner_email_domain: true` (emails → local part) or `owner_email_domain: acme.com`
(bare usernames → emails) to normalize contract identifiers to your convention; explicit
`urn:li:corpuser:` / `urn:li:corpGroup:` values pass through untouched. When ingesting
through a DataHub graph, owners that don't resolve are flagged in the run report
(`report.owners_unresolved`, one warning per unique owner) — the reference is still
emitted and becomes functional as soon as the user is provisioned.

#### I edited owners on a logical dataset and the next ingest reverted them

By default, contract-level ownership replicates on every ingest, so any UI edits are
overwritten. Set `replicate_contract_metadata: false` in the source config to switch to
first-sight-only emission, which preserves UI edits after the initial ingest. The same
setting also governs top-level tags.

---

For the canonical ODCS spec and JSON Schemas, see the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).
For an audience-friendly entry point and adopter list, see
[bitol.io](https://bitol.io/).


### Code Coordinates
- Class Name: `datahub.ingestion.source.odcs.odcs_source.ODCSSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/odcs/odcs_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Open Data Contract Standard, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
