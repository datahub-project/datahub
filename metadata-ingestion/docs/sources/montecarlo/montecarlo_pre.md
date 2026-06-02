### Overview

Monte Carlo is a data observability platform that monitors data pipelines and tables for freshness,
volume, schema, and field-level anomalies. It also lets teams author custom SQL rules and threshold
monitors.

This module ingests Monte Carlo monitors and their evaluation results as DataHub
[Assertions](https://docs.datahub.com/docs/assertions/), so teams can view data quality status
directly alongside dataset metadata in DataHub.

### Prerequisites

In order to ingest metadata from Monte Carlo, you will need:

- A Monte Carlo Cloud account (this connector does not support self-hosted/on-prem variants).
- An API key pair (`mcd_id` + `mcd_token`) with read access to monitors, custom rules, alerts and the catalog.
- A `connection_to_platform_map` entry for each Monte Carlo warehouse you want ingested, so monitored-asset URNs align with the URNs emitted by your warehouse sources.
