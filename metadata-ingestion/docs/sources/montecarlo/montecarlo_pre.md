### Overview

[Monte Carlo](https://www.montecarlodata.com/) is a data observability platform that monitors
warehouse and lake tables for freshness, volume, schema and field-quality issues and raises
alerts/incidents when they breach.

This connector ingests Monte Carlo **monitors**, **custom (SQL) rules** and **alerts/incidents** and
models them as DataHub **Assertions**, so the native "Validation" tab on a dataset reflects Monte
Carlo's observability coverage and incident history.

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
