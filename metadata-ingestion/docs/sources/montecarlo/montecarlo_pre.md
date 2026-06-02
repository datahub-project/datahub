### Prerequisites

In order to ingest metadata from Monte Carlo, you will need:

- A Monte Carlo Cloud account (this connector does not support self-hosted/on-prem variants).
- An API key pair (`mcd_id` + `mcd_token`) with read access to monitors, custom rules, alerts and the catalog.
- A `connection_to_platform_map` entry for each Monte Carlo warehouse you want ingested, so monitored-asset URNs align with the URNs emitted by your warehouse sources.
