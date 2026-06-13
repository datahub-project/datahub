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
concrete table via `getTable` and maps the warehouse connection type to a DataHub platform. Use
`connection_to_platform_map` to pin the `platform`, `platform_instance` and `env` for each Monte
Carlo warehouse so the resulting dataset URNs line up with the URNs emitted by your warehouse
sources (Snowflake, BigQuery, etc.).
