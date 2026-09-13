


# Monte Carlo

## Overview

[Monte Carlo](https://www.montecarlodata.com/) is a data observability platform that monitors warehouse and lake tables for freshness, volume, schema and field-quality issues and raises alerts/incidents when they breach.

This connector ingests Monte Carlo **monitors**, **custom (SQL) rules** and **alerts/incidents** and models them as DataHub **Assertions**, so the native "Validation" tab on a dataset reflects Monte Carlo's observability coverage and incident history.

## Concept Mapping

| Monte Carlo Concept    | DataHub Concept                                                                           | Notes                                                                                                       |
| ---------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `"montecarlo"`         | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                                                                                                             |
| Monitor                | [Assertion](/docs/generated/metamodel/entities/assertion/)        | One `CUSTOM` assertion per monitor; native `comparisons` mapped to structured `CustomAssertionInfo` fields. |
| Custom (SQL) rule      | [Assertion](/docs/generated/metamodel/entities/assertion/)        | One `CUSTOM` assertion per rule; SQL captured in `customAssertion.logic`.                                   |
| Monitored asset (MCON) | [Dataset](/docs/generated/metamodel/entities/dataset/)            | Resolved via `getTable` and `connection_to_platform_map`.                                                   |
| Alert / Incident       | Assertion Run Event                                                                       | Emitted as an `AssertionRunEvent` failure on the corresponding assertion.                                   |

Every monitor/rule is modeled as a `CUSTOM` assertion (matching the established connector pattern, e.g. Snowflake DMFs and dbt tests). The Monte Carlo native `comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields (`scope`, `operator`, `aggregation`, `fields`, `parameters`) so Monte Carlo assertions render through the same shared description component as dbt and Great Expectations. The native type, severity, resource id and data-quality dimension are carried on `nativeType`/`nativeParameters`; `customProperties` keeps only the `mc_monitor_uuid` correlation key.


## Module `montecarlo`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Monitor/rule descriptions become assertion descriptions. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled via connection_to_platform_map. |

### Overview

[Monte Carlo](https://www.montecarlodata.com/) is a data observability platform that monitors
warehouse and lake tables for freshness, volume, schema and field-quality issues and raises
alerts/incidents when they breach.

This connector ingests Monte Carlo **monitors**, **custom (SQL) rules** and **alerts/incidents** and
models them as DataHub **Assertions**, so the native "Validation" tab on a dataset reflects Monte
Carlo's observability coverage and incident history. Optionally, it can also ingest Monte Carlo's
monitor **run history** with the **measured metric values** behind each run, so a dataset's
assertion timeline shows both failures (from alerts) and successes (with the actual measured
numbers).

### Prerequisites

In order to ingest metadata from Monte Carlo, you will need:

- A Monte Carlo Cloud account (this connector does not support self-hosted/on-prem variants).
- An API key pair (`mcd_id` + `mcd_token`) with read access to monitors, custom rules, alerts and
  the catalog. Create one in the Monte Carlo UI under **Settings → API** (see the
  [Monte Carlo API docs](https://docs.getmontecarlo.com/docs/using-the-api)).
- A `connection_to_platform_map` entry for each Monte Carlo warehouse you want ingested, so
  monitored-asset URNs align with the URNs emitted by your warehouse sources.

#### Cross-platform URN mapping

A Monte Carlo MCON does not encode the DataHub platform. The connector resolves each MCON to a
concrete table via `getTable` and uses `connection_to_platform_map` to pin the `platform`,
`platform_instance` and `env` for each Monte Carlo warehouse so the resulting dataset URNs line up
with the URNs emitted by your warehouse sources (Snowflake, BigQuery, etc.). This explicit map is
the default and safest resolution path: an asset whose warehouse is not in the map is skipped with
a warning.

If maintaining an entry per warehouse is impractical, you can opt into automatic inference with
`auto_map_connection_types: true`. When enabled, the connector infers the DataHub platform for
warehouses missing from `connection_to_platform_map` from the Monte Carlo warehouse connection type
(`snowflake`, `bigquery`, `redshift`, ...), falling back to `default_platform` for unrecognized
connection types. The inferred dataset URN uses the top-level `platform_instance` and `env` (not
per-warehouse values), so this is only safe for single-instance-per-platform setups — in
multi-instance setups it can attach assertions to the wrong dataset. Prefer
`connection_to_platform_map` where possible.

Each key in `connection_to_platform_map` is a Monte Carlo **warehouse resource UUID** — not the
warehouse's display name. You can find it in any of these ways:

- **From an asset's MCON:** the resource UUID is the third `++`-delimited segment. In
  `MCON++<account>++<resource-uuid>++table++<db.schema.table>`, the key is `<resource-uuid>`.
- **From the Monte Carlo UI:** open **Settings → Integrations**, select the warehouse, and copy the
  resource UUID shown for that connection.
- **From the API:** query your warehouses (for example `getUser { account { warehouses { uuid name connectionType } } }`
  in the Monte Carlo GraphQL playground); each warehouse's `uuid` is the value to use as the key.

#### Auto-mapped connection types

When a warehouse is **not** listed in `connection_to_platform_map`, the connector auto-maps its
Monte Carlo `connectionType` (the `WarehouseModelConnectionType` enum value returned by `getTable`)
to a DataHub platform. The supported auto-mappings are:

| Monte Carlo connection type                              | DataHub platform |
| -------------------------------------------------------- | ---------------- |
| `snowflake`                                              | `snowflake`      |
| `bigquery`                                               | `bigquery`       |
| `redshift`                                               | `redshift`       |
| `mysql`                                                  | `mysql`          |
| `oracle`                                                 | `oracle`         |
| `teradata`                                               | `teradata`       |
| `clickhouse`                                             | `clickhouse`     |
| `dremio`                                                 | `dremio`         |
| `db2`                                                    | `db2`            |
| `starburst_enterprise`                                   | `trino`          |
| `starburst_galaxy`                                       | `trino`          |
| `databricks` / `databricks-sql` / `databricks-metastore` | `databricks`     |
| `spark`                                                  | `spark`          |
| `presto`                                                 | `presto`         |
| `hive`                                                   | `hive`           |
| `glue`                                                   | `glue`           |
| `athena`                                                 | `athena`         |

Any other connection type (and warehouses whose `connectionType` Monte Carlo reports as
`transactional_db`) is **not** auto-mapped: the connector logs a warning and skips the asset rather
than guessing a platform. `transactional_db` is a category that spans PostgreSQL, SQL Server,
Synapse, SAP HANA, Azure SQL and others, so it cannot be mapped to a single DataHub platform — you
must add an explicit `connection_to_platform_map` entry for each such warehouse. The same applies to
Azure SQL and SAP HANA, which Monte Carlo reports under `transactional_db` rather than as a distinct
type.


### Install the Plugin
```shell
pip install 'acryl-datahub[montecarlo]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: montecarlo
  config:
    # --- Authentication ---
    api_id: ${MCD_ID} # Monte Carlo API key id (mcd_id)
    api_token: ${MCD_TOKEN} # Monte Carlo API key token (mcd_token)
    # api_endpoint: https://api.getmontecarlo.com/graphql # override the MCD endpoint

    # --- Warehouse -> DataHub platform mapping ---
    # Map each Monte Carlo warehouse/resource UUID to a DataHub platform so the
    # monitored-asset URNs match those emitted by your warehouse sources. The
    # key is the warehouse resource UUID (the resource segment of an asset's
    # MCON; visible on the warehouse in the Monte Carlo settings UI).
    connection_to_platform_map:
      "<mc-warehouse-uuid>":
        platform: snowflake # DataHub platform name (snowflake, bigquery, redshift, ...)
        platform_instance: prod # warehouse platform instance (NOT Monte Carlo's)
        env: PROD
        # Override URN casing for this warehouse only. The Snowflake connector
        # lowercases dataset URNs by default, so set true here to make Monte
        # Carlo's assertion URNs attach to the same datasets. Set false only
        # for a case-preserving Snowflake/Redshift deployment whose warehouse
        # source runs with convert_urns_to_lowercase: false. Leave unset to
        # inherit the top-level convert_urns_to_lowercase flag.
        convert_urns_to_lowercase: true

    # Fallbacks for warehouses NOT listed in the map above. These stamp the
    # warehouse dataset URN (NOT Monte Carlo's own assertion entity).
    # default_platform: postgres # fallback platform for unmapped connection types
    # target_platform_instance: prod # warehouse platform instance for auto-mapped warehouses
    # target_env: PROD # env for auto-mapped warehouses (defaults to top-level env)

    # Auto-map a warehouse's Monte Carlo connectionType to a DataHub platform
    # when the warehouse is not in connection_to_platform_map. Set to false to
    # require an explicit mapping for every warehouse (unmapped ones are
    # skipped with a warning). default_platform is only consulted when this is
    # enabled.
    auto_map_connection_types: true

    # --- What to ingest ---
    include_assertions: true # monitors + custom rules -> Assertion entities
    include_alerts: true # alerts/incidents -> AssertionRunEvent failures
    alerts_lookback_days: 30 # how far back to fetch alerts (default: 30 days)
    # Opt in to also create Incident entities (Incidents tab). Off by default —
    # see the Limitations section (incidents stay ACTIVE with no RESOLVED
    # signal). Uncomment to enable once you have a workflow that manages
    # incident resolution in DataHub separately.
    # emit_incidents_on_failure: true

    # Ingest monitor run history + measured metric values as SUCCESS
    # AssertionRunEvents. Leave unset (None) to keep the FAILURE-only behaviour.
    run_events_lookback_days: 7 # query window in days (must be a positive integer)
    run_events_first: 5 # max runs fetched per monitor (default: 5)

    # --- Filtering ---
    # Regex allow/deny patterns for monitor/rule names.
    # monitor_pattern:
    #   allow:
    #     - ".*freshness.*"
    # Regex allow/deny patterns for Monte Carlo monitor types (FRESHNESS, VOLUME, ...).
    # monitor_type_pattern:
    #   deny:
    #     - "VOLUME"
    # Scope ingestion to specific Monte Carlo domain UUIDs.
    # domain_ids:
    #   - "<mc-domain-uuid>"

    # --- Rate limiting (client-side) ---
    # Sustained token bucket refill rate (requests/second). Leave unset to disable.
    # rate_limit_requests_per_second: 5
    # Token bucket capacity — burst size above the sustained rate.
    # rate_limit_burst: 10
    # Max API calls per UTC calendar day (per-run cap; exceeding it fails the run).
    # rate_limit_daily: 5000

    # --- URN casing (recipe level) ---
    # Forces lowercase dataset URNs everywhere when true. Per-warehouse
    # convert_urns_to_lowercase overrides (above) take precedence. Leave unset
    # to preserve case (set true for Snowflake/Redshift whose source lowercases).
    # convert_urns_to_lowercase: true

    # --- Stateful ingestion / soft-deletion ---
    # Opt in to soft-delete assertions that no longer exist in Monte Carlo.
    # Leave disabled until you have confirmed the run is healthy end-to-end;
    # a partial run with transient errors skips deletion automatically.
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
| <div className="path-line"><span className="path-main">api_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Monte Carlo API key id (the ``mcd_id`` of an API key pair).  |
| <div className="path-line"><span className="path-main">api_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Monte Carlo API key token (the ``mcd_token`` of an API key pair).  |
| <div className="path-line"><span className="path-main">alerts_lookback_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many days back to fetch alerts/incidents for. Only applies when include_alerts is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">api_endpoint</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override for the Monte Carlo MCD GraphQL endpoint. Defaults to the endpoint baked into the pycarlo client when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">auto_map_connection_types</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, infer the DataHub platform for warehouses missing from connection_to_platform_map from the Monte Carlo warehouse connection type (snowflake, bigquery, redshift, ...), falling back to default_platform for unrecognized connection types. The inferred dataset URN uses the top-level platform_instance and env (not per-warehouse values), so this is only safe for single-instance-per-platform setups — in multi-instance setups it can attach assertions to the wrong dataset. Disabled by default; prefer connection_to_platform_map where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">default_platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Fallback DataHub platform used only when auto_map_connection_types is enabled and a warehouse's connection type is not in the built-in connection-type map. Leave unset to skip (and warn about) such warehouses. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_incidents_on_failure</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit a DataHub Incident entity (``urn:li:incident:…``) for each Monte Carlo alert/incident, in addition to the AssertionRunEvent failure. The incident links back to the assertion via ``IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`` so the Incidents tab on the dataset shows the failure history. Standard DataHub entity — works regardless of edition. Re-emitting the same alert is idempotent because the incident URN is derived from a hash of (assertion_urn, alert_uuid). Disabled by default: Monte Carlo alerts resolve over time but the connector fetches them only within alerts_lookback_days and has no signal to emit an IncidentState.RESOLVED transition, so enabling this can accumulate stale ACTIVE incidents. Enable it only if your workflow tolerates that and manages incident resolution separately. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_alerts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Monte Carlo alerts/incidents as assertion run events (failures). Requires include_assertions, since run events attach to the assertions built from monitors. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_assertions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Monte Carlo monitors and custom rules as DataHub assertions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">rate_limit_burst</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Token bucket capacity — the number of requests that can burst above the sustained rate before throttling kicks in. Only used when rate_limit_requests_per_second is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">rate_limit_daily</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum API calls allowed per UTC calendar day, matching Monte Carlo's own daily-limit reset behavior. Exceeding it fails the run rather than blocking until the next day. This is a per-run cap, not a true cross-run daily budget: it is not shared or coordinated across separate/overlapping ingestion runs, so it cannot prevent the combined total across runs from exceeding this value. Leave unset to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">rate_limit_requests_per_second</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Sustained token bucket refill rate, in requests/second. Leave unset to disable client-side rate limiting entirely. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">run_events_first</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of most-recent runs to fetch per monitor (the `first` arg on getJobExecutions). All SUCCESS runs in the page are emitted as AssertionRunEvents; the latest one carries the measured metric value from getMetricsV4. Only applies when run_events_lookback_days is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">run_events_lookback_days</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Ingest Monte Carlo monitor run history (getJobExecutions) plus measured metric values (getMetricsV4) as AssertionRunEvents. When set to a positive integer N, emits the latest SUCCESS run(s) per monitor (carrying the measured value on AssertionResult) for runs within the last N days. Leave unset (None) to disable — the alert-driven FAILURE-only path (include_alerts) is the historical behaviour. Requires include_assertions. Bounds the query window, not the run count (run_events_first caps the count). Adds ~1 getJobExecutions + ~1 getMetricsV4 call per ingested monitor per run; set rate_limit_daily to bound this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">target_env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment to stamp on the warehouse dataset URNs built for auto-mapped warehouses (those not in connection_to_platform_map). Separate from Monte Carlo's own env so the warehouse URN namespace can be controlled independently. When unset, falls back to the top-level env (the values usually coincide). For warehouses listed in connection_to_platform_map, set the env per entry instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">target_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance to stamp on the warehouse dataset URNs built for warehouses that are auto-mapped (not listed in connection_to_platform_map) or that fall back to default_platform. This is the warehouse platform's instance, NOT Monte Carlo's own — the top-level platform_instance field is Monte Carlo's and must not leak onto warehouse dataset URNs (it would attach assertions to datasets that do not exist). For warehouses listed in connection_to_platform_map, set the instance per entry instead. Mirrors the dbt/sqlmesh target_platform_instance convention. Leave unset for no platform instance on auto-mapped warehouse URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,MonteCarloPlatformDetail)</span></div> | Maps a Monte Carlo warehouse/connection to a DataHub platform. <br />  <br /> Monte Carlo identifies the warehouse a monitored asset lives in by a <br /> resource/warehouse UUID, but it does not expose the DataHub platform name <br /> (e.g. ``snowflake``) directly. This mapping lets users pin the platform, <br /> platform instance and environment used to build the dataset URN so it lines <br /> up with the URNs emitted by the corresponding warehouse source. Reuses the <br /> same ``platform_instance``/``env`` fields (and validation) as other sources' <br /> connection-to-platform mappings (see e.g. qlik_sense, sigma, trino).  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub platform name for assets in this Monte Carlo warehouse, e.g. 'snowflake', 'bigquery', 'redshift', 'databricks'.  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Override the dataset URN casing for this warehouse only. Set to true to force lowercase, false to preserve the case Monte Carlo reports (needed for case-preserving Snowflake/Redshift deployments whose warehouse source runs with convert_urns_to_lowercase=false). Leave unset to inherit the top-level convert_urns_to_lowercase flag, which forces lowercase everywhere when true and preserves case otherwise. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">domain_ids</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Optional list of Monte Carlo domain UUIDs to scope ingestion to. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain_ids.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">monitor_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">monitor_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">monitor_type_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">monitor_type_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration. Enables soft-deletion of assertions whose Monte Carlo monitor no longer exists. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "MonteCarloPlatformDetail": {
      "additionalProperties": false,
      "description": "Maps a Monte Carlo warehouse/connection to a DataHub platform.\n\nMonte Carlo identifies the warehouse a monitored asset lives in by a\nresource/warehouse UUID, but it does not expose the DataHub platform name\n(e.g. ``snowflake``) directly. This mapping lets users pin the platform,\nplatform instance and environment used to build the dataset URN so it lines\nup with the URNs emitted by the corresponding warehouse source. Reuses the\nsame ``platform_instance``/``env`` fields (and validation) as other sources'\nconnection-to-platform mappings (see e.g. qlik_sense, sigma, trino).",
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
        "platform": {
          "description": "DataHub platform name for assets in this Monte Carlo warehouse, e.g. 'snowflake', 'bigquery', 'redshift', 'databricks'.",
          "title": "Platform",
          "type": "string"
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
          "description": "Override the dataset URN casing for this warehouse only. Set to true to force lowercase, false to preserve the case Monte Carlo reports (needed for case-preserving Snowflake/Redshift deployments whose warehouse source runs with convert_urns_to_lowercase=false). Leave unset to inherit the top-level convert_urns_to_lowercase flag, which forces lowercase everywhere when true and preserves case otherwise.",
          "title": "Convert Urns To Lowercase"
        }
      },
      "required": [
        "platform"
      ],
      "title": "MonteCarloPlatformDetail",
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
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
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
      "description": "Stateful ingestion configuration. Enables soft-deletion of assertions whose Monte Carlo monitor no longer exists."
    },
    "api_id": {
      "description": "Monte Carlo API key id (the ``mcd_id`` of an API key pair).",
      "title": "Api Id",
      "type": "string"
    },
    "api_token": {
      "description": "Monte Carlo API key token (the ``mcd_token`` of an API key pair).",
      "format": "password",
      "title": "Api Token",
      "type": "string",
      "writeOnly": true
    },
    "api_endpoint": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Override for the Monte Carlo MCD GraphQL endpoint. Defaults to the endpoint baked into the pycarlo client when unset.",
      "title": "Api Endpoint"
    },
    "connection_to_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/MonteCarloPlatformDetail"
      },
      "default": {},
      "description": "Maps a Monte Carlo warehouse resource UUID to a DataHub platform, platform instance and env, used to build dataset URNs for monitored assets so they line up with the warehouse source's URNs. The key is the warehouse's resource UUID (the resource segment of an asset's MCON, ``MCON++<account>++<resource-uuid>++table++...``; also visible on the warehouse in the Monte Carlo settings UI), not its display name. This is the default and safest resolution path \u2014 assets whose warehouse is not in this map are skipped with a warning unless auto_map_connection_types is enabled.",
      "title": "Connection To Platform Map",
      "type": "object"
    },
    "auto_map_connection_types": {
      "default": false,
      "description": "When enabled, infer the DataHub platform for warehouses missing from connection_to_platform_map from the Monte Carlo warehouse connection type (snowflake, bigquery, redshift, ...), falling back to default_platform for unrecognized connection types. The inferred dataset URN uses the top-level platform_instance and env (not per-warehouse values), so this is only safe for single-instance-per-platform setups \u2014 in multi-instance setups it can attach assertions to the wrong dataset. Disabled by default; prefer connection_to_platform_map where possible.",
      "title": "Auto Map Connection Types",
      "type": "boolean"
    },
    "default_platform": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Fallback DataHub platform used only when auto_map_connection_types is enabled and a warehouse's connection type is not in the built-in connection-type map. Leave unset to skip (and warn about) such warehouses.",
      "title": "Default Platform"
    },
    "target_platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Platform instance to stamp on the warehouse dataset URNs built for warehouses that are auto-mapped (not listed in connection_to_platform_map) or that fall back to default_platform. This is the warehouse platform's instance, NOT Monte Carlo's own \u2014 the top-level platform_instance field is Monte Carlo's and must not leak onto warehouse dataset URNs (it would attach assertions to datasets that do not exist). For warehouses listed in connection_to_platform_map, set the instance per entry instead. Mirrors the dbt/sqlmesh target_platform_instance convention. Leave unset for no platform instance on auto-mapped warehouse URNs.",
      "title": "Target Platform Instance"
    },
    "target_env": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Environment to stamp on the warehouse dataset URNs built for auto-mapped warehouses (those not in connection_to_platform_map). Separate from Monte Carlo's own env so the warehouse URN namespace can be controlled independently. When unset, falls back to the top-level env (the values usually coincide). For warehouses listed in connection_to_platform_map, set the env per entry instead.",
      "title": "Target Env"
    },
    "include_assertions": {
      "default": true,
      "description": "Ingest Monte Carlo monitors and custom rules as DataHub assertions.",
      "title": "Include Assertions",
      "type": "boolean"
    },
    "include_alerts": {
      "default": true,
      "description": "Ingest Monte Carlo alerts/incidents as assertion run events (failures). Requires include_assertions, since run events attach to the assertions built from monitors.",
      "title": "Include Alerts",
      "type": "boolean"
    },
    "alerts_lookback_days": {
      "default": 30,
      "description": "How many days back to fetch alerts/incidents for. Only applies when include_alerts is enabled.",
      "exclusiveMinimum": 0,
      "title": "Alerts Lookback Days",
      "type": "integer"
    },
    "emit_incidents_on_failure": {
      "default": false,
      "description": "Emit a DataHub Incident entity (``urn:li:incident:\u2026``) for each Monte Carlo alert/incident, in addition to the AssertionRunEvent failure. The incident links back to the assertion via ``IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`` so the Incidents tab on the dataset shows the failure history. Standard DataHub entity \u2014 works regardless of edition. Re-emitting the same alert is idempotent because the incident URN is derived from a hash of (assertion_urn, alert_uuid). Disabled by default: Monte Carlo alerts resolve over time but the connector fetches them only within alerts_lookback_days and has no signal to emit an IncidentState.RESOLVED transition, so enabling this can accumulate stale ACTIVE incidents. Enable it only if your workflow tolerates that and manages incident resolution separately.",
      "title": "Emit Incidents On Failure",
      "type": "boolean"
    },
    "run_events_lookback_days": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Ingest Monte Carlo monitor run history (getJobExecutions) plus measured metric values (getMetricsV4) as AssertionRunEvents. When set to a positive integer N, emits the latest SUCCESS run(s) per monitor (carrying the measured value on AssertionResult) for runs within the last N days. Leave unset (None) to disable \u2014 the alert-driven FAILURE-only path (include_alerts) is the historical behaviour. Requires include_assertions. Bounds the query window, not the run count (run_events_first caps the count). Adds ~1 getJobExecutions + ~1 getMetricsV4 call per ingested monitor per run; set rate_limit_daily to bound this.",
      "title": "Run Events Lookback Days"
    },
    "run_events_first": {
      "default": 5,
      "description": "Maximum number of most-recent runs to fetch per monitor (the `first` arg on getJobExecutions). All SUCCESS runs in the page are emitted as AssertionRunEvents; the latest one carries the measured metric value from getMetricsV4. Only applies when run_events_lookback_days is set.",
      "exclusiveMinimum": 0,
      "title": "Run Events First",
      "type": "integer"
    },
    "monitor_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for monitor/rule names to filter in/out."
    },
    "monitor_type_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for Monte Carlo monitor types (e.g. 'FRESHNESS', 'VOLUME') to filter in/out."
    },
    "domain_ids": {
      "default": [],
      "description": "Optional list of Monte Carlo domain UUIDs to scope ingestion to.",
      "items": {
        "type": "string"
      },
      "title": "Domain Ids",
      "type": "array"
    },
    "rate_limit_requests_per_second": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "number"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Sustained token bucket refill rate, in requests/second. Leave unset to disable client-side rate limiting entirely.",
      "title": "Rate Limit Requests Per Second"
    },
    "rate_limit_burst": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Token bucket capacity \u2014 the number of requests that can burst above the sustained rate before throttling kicks in. Only used when rate_limit_requests_per_second is set.",
      "title": "Rate Limit Burst"
    },
    "rate_limit_daily": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Maximum API calls allowed per UTC calendar day, matching Monte Carlo's own daily-limit reset behavior. Exceeding it fails the run rather than blocking until the next day. This is a per-run cap, not a true cross-run daily budget: it is not shared or coordinated across separate/overlapping ingestion runs, so it cannot prevent the combined total across runs from exceeding this value. Leave unset to disable.",
      "title": "Rate Limit Daily"
    }
  },
  "required": [
    "api_id",
    "api_token"
  ],
  "title": "MonteCarloSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Assertion modeling

Every Monte Carlo monitor and custom SQL rule is modeled as a `CUSTOM` assertion, and its native
`comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields so Monte Carlo
assertions render through the same shared description component as dbt and Great Expectations.

For each monitor/rule, the first `comparisons` entry drives the structured fields:

- **`scope`** — `DATASET_COLUMN` when the comparison carries a column reference (`field`/`fields`),
  else `DATASET_ROWS` (table-level and row-predicate checks). This mirrors dbt's column-vs-row
  heuristic.
- **`operator`** — the MC comparison operator mapped to `AssertionStdOperator` (`EQ`→`EQUAL_TO`,
  `GT`/`GTE`/`LT`/`LTE`/`NEQ`, `INSIDE_RANGE`→`BETWEEN`, `IS_NULL`→`NULL`, `IS_NOT_NULL`→`NOT_NULL`).
  Operators with no clean DataHub equivalent (`AUTO*`, `NOOP`, `OUTSIDE_RANGE`) fall back to
  `_NATIVE_`.
- **`aggregation`** — the MC metric mapped to `AssertionStdAggregation` (`row_count`→`ROW_COUNT`,
  `distinct_count`→`UNIQUE_COUNT`, `null_count`→`NULL_COUNT`, `null_rate`→`NULL_PROPORTION`,
  `min`/`max`/`mean`/`median`/`stddev`/`sum`). Unmapped metrics fall back to `_NATIVE_`.
- **`fields`** — schema-field URNs built from the comparison's `field`/`fields`.
- **`parameters`** — thresholds mapped to `AssertionStdParameters` (`BETWEEN`→`minValue`/`maxValue`;
  scalar comparisons → `value`).

The Monte Carlo native type is preserved in `nativeType`, and native fields (severity, data-quality
dimension, resource id, comparison type, metric) are carried in `nativeParameters`. `customProperties`
keeps only the DataHub-internal `mc_monitor_uuid` correlation key. For custom SQL rules, the raw
SQL expression is captured in `customAssertion.logic`.

Monte Carlo rules can carry several independent comparisons, but DataHub's assertion model is
single-comparison. The connector maps `comparisons[0]` onto the structured fields above and folds
any remaining comparisons into `customAssertion.logic` as JSON, so a compound rule is still fully
represented. This preserves the one-monitor → one-assertion-URN scheme, so alert and run-event
wiring is unchanged.

Monitors for which no `comparisons` are returned (or the comparisons are malformed) fall back to
`scope = DATASET_ROWS` with `_NATIVE_` operator/aggregation, so they still render through the
shared path and carry their native fields on `nativeType`/`nativeParameters`.

#### Alert and incident ingestion

Monte Carlo alerts and incidents are ingested as `AssertionRunEvent` failures on their
corresponding assertion. Each event carries a timestamp, the Monte Carlo alert ID, and the
alert's native severity/priority/sub-type on `nativeResults`.

When `emit_incidents_on_failure` is enabled (disabled by default), the connector also creates a DataHub
`Incident` entity (`urn:li:incident:…`) for each alert/incident. The incident links back to the
assertion via `IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`, so the failure
appears on the **Incidents** tab of the monitored dataset in addition to the Assertions tab. The
incident URN is derived deterministically from a hash of `(assertion_urn, alert_uuid)`, so
re-ingesting the same alert updates the existing incident rather than creating a duplicate. Set
`emit_incidents_on_failure: true` to enable incident entities. It is off by default because Monte
Carlo alerts resolve over time but the connector has no signal to emit an
`IncidentState.RESOLVED` transition (see [Limitations](#limitations)), so enabling it can
accumulate stale `ACTIVE` incidents.

#### Run history and measured metric values

By default the connector emits only the alert-driven `FAILURE` run events above. To also ingest
Monte Carlo's monitor **run history** (`getJobExecutions`) together with the **measured metric
values** (`getMetricsV4`) — i.e. the actual numbers Monte Carlo computed for each monitor run —
set `run_events_lookback_days` to a positive integer N.

When enabled, for each ingested monitor the connector:

- Fetches the most recent runs (capped by `run_events_first`, default `5`) within the last N days.
- Emits every `SUCCESS` run as an `AssertionRunEvent` with `status = COMPLETE` and
  `result.type = SUCCESS`. The **latest** SUCCESS run carries the measured metric value on
  `AssertionResult` — standard metrics land on the typed slots (`rowCount`, `missingCount`,
  `unexpectedCount`, `actualAggValue`) and the rest fall back to `nativeResults`. Older SUCCESS
  runs carry only per-run execution metadata (`totalResultCount`, `evaluatedRecordCount`,
  `exceptions`).
- Leaves `FAILURE` runs to the alert-driven path above (no duplicate events).

`run_events_lookback_days` bounds the **query window**, not the run count — `run_events_first`
caps the count. Enabling this adds roughly one `getJobExecutions` call plus one `getMetricsV4` call
per metric per ingested monitor; set `rate_limit_daily` to bound the extra API spend. Leave
`run_events_lookback_days` unset (`None`) to keep the historical FAILURE-only behaviour.

Run events are emitted with `is_primary_source = False`, so stale entity removal never touches
them — the assertion entity itself is still subject to soft-deletion (via the monitor-definition
path) if the monitor disappears from Monte Carlo, but its run history is preserved.

### Limitations

- **Run history is opt-in:** Without `run_events_lookback_days`, the connector emits only
  `FAILURE` run events (from alerts/incidents); periodic `SUCCESS` events and measured metric
  values are not synthesized. Set `run_events_lookback_days` to ingest them.
- **Best-effort metric correlation:** `getMetricsV4` does not populate `jobExecutionUuid` for
  table-level metrics, so a per-run join is not possible. The measured value is attached to the
  latest SUCCESS run as a best-effort temporal correlation ("most recent measurement" on "most
  recent successful run"), not a proven same-run match.
- **MCON resolution:** Each monitored asset requires one `getTable` call to resolve its MCON to a
  warehouse table (results are cached per MCON). Assets whose warehouse is not in
  `connection_to_platform_map` are skipped with a warning unless `auto_map_connection_types` is
  enabled, in which case the platform is inferred from the warehouse connection type (with
  `default_platform` as the fallback for unrecognized types).
- **Assertion typing:** All monitors and rules are modeled as `CUSTOM` assertions. Their native
  `comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields
  (`scope`/`operator`/`aggregation`/`fields`/`parameters`) and the native type/parameters are
  carried on `nativeType`/`nativeParameters`, but the assertions are not coerced into DataHub's
  typed freshness/volume/SQL/field assertion schemas.
- **Monte Carlo Cloud only:** Requires a Monte Carlo Cloud account and API key pair. Self-hosted
  deployments are not supported.
- **Incidents stay ACTIVE:** When `emit_incidents_on_failure` is enabled, each Monte Carlo
  alert creates a DataHub `Incident` in the `ACTIVE` state. The connector fetches alerts only
  within `alerts_lookback_days`, and resolved alerts typically no longer appear in Monte Carlo's
  alert feed, so there is no signal to emit an `IncidentState.RESOLVED` transition. As a result,
  DataHub incidents can accumulate in the `ACTIVE` state after the underlying Monte Carlo alert has
  resolved. This is why `emit_incidents_on_failure` is disabled by default; leave it off if your
  workflow does not tolerate stale active incidents, and only enable it if you manage incident
  resolution in DataHub separately.

### Troubleshooting

#### Monitored assets are skipped with a warning

If you see warnings like `Could not resolve MCON to a DataHub dataset URN`, the warehouse for that
asset is not in `connection_to_platform_map`. Add a mapping entry for the warehouse resource UUID
shown in the warning, or enable `auto_map_connection_types` to infer the platform from the
warehouse connection type (falling back to `default_platform` for unrecognized types).

#### Assertion URNs do not match your warehouse source

Assertion URNs are keyed from the Monte Carlo monitor ID, but they target the dataset URN resolved
via `connection_to_platform_map` (or auto-mapped from the warehouse connection type, falling back to
`default_platform`). If the `platform`, `platform_instance`, or `env` values differ from those used by
your warehouse source connector, the assertions will not appear on the correct dataset. Align the
values in `connection_to_platform_map` with the config of your warehouse source.

A few specifics that commonly cause silent mis-attachment:

- **`platform_instance` is Monte Carlo's, not the warehouse's.** The top-level `platform_instance`
  field is Monte Carlo's own instance (it stamps the `dataPlatformInstance` aspect on the assertion
  entity). It is **not** applied to warehouse dataset URNs. For warehouses listed in
  `connection_to_platform_map`, set the instance per entry. For auto-mapped warehouses (those not in
  the map), use `target_platform_instance` instead — leaving it unset means no platform instance on
  those URNs, which is safer than guessing.
- **`env` for auto-mapped warehouses.** `target_env` controls the env on auto-mapped warehouse URNs
  independently of Monte Carlo's own `env`; when unset it falls back to the top-level `env` (the values
  usually coincide). Set it explicitly if your warehouse source uses a different env.
- **Identifier casing.** Dataset URN casing is controlled at recipe level, not
  hardcoded per platform. The per-warehouse `convert_urns_to_lowercase` override on
  the matching `connection_to_platform_map` entry takes precedence when set — set
  it to `false` for a case-preserving Snowflake/Redshift deployment whose warehouse
  source runs with `convert_urns_to_lowercase: false`, so the assertion targets the
  same-cased dataset. Otherwise the top-level `convert_urns_to_lowercase` flag applies:
  `true` forces lowercase everywhere; leaving it unset preserves case. Set the
  top-level flag to `true` if your warehouse source lowercases (e.g. Snowflake with
  `convert_urns_to_lowercase: true`).
- **Malformed table ids.** A Monte Carlo `full_table_id` that does not resolve to
  `database.schema.table` (three dot-separated segments) is skipped with a warning rather than
  producing a URN for a dataset that does not exist.

#### No assertions appear after ingestion

Verify that:

1. The API key has read access to monitors, custom rules, and alerts in the Monte Carlo UI.
2. At least one monitor is active and has fired an alert (the connector ingests only monitors that
   have associated assets and alerts).
3. The `connection_to_platform_map` covers the warehouse connections used by your monitored assets.

#### Stateful ingestion and soft-deletion

Stateful ingestion (the `stateful_ingestion` config block) is **opt-in** — the starter recipes
ship it commented out. When enabled, assertions that no longer exist in Monte Carlo are
soft-deleted from DataHub at the end of a run.

Two safety guards prevent soft-deletion from a bad run:

- **Zero-assertion guard.** If the run attempted to build assertions but emitted none
  (every monitor failed to resolve), the run records a failure and stale removal is skipped.
- **Partial-failure guard.** If any monitor or custom rule failed to build due to a transient
  error (network blip, API error, unexpected exception during `getTable`), the run records a
  failure and stale removal is skipped — even if most monitors built successfully. This
  covers the band the zero-assertion guard misses (e.g. 40 of 100 monitors hit a transient
  `getTable` error). Permanent failures (the table is genuinely gone, or the platform is
  unmapped) do **not** trip this guard — those are legitimate deletions.

Transient `getTable` failures are also **not cached**, so a later monitor sharing the same
MCON retries instead of inheriting a stale `None`. Leave `stateful_ingestion` disabled until
you have confirmed a run is healthy end-to-end; the guards make it safe to enable afterwards.


### Code Coordinates
- Class Name: `datahub.ingestion.source.montecarlo.source.MonteCarloSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/montecarlo/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Monte Carlo, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
