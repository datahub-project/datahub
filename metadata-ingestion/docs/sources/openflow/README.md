## Overview

Snowflake Openflow is Snowflake's managed integration service, built on Apache NiFi. This source
catalogs Openflow's own objects — deployments, runtimes and connectors — and emits table-level
lineage from each connector to the Snowflake tables it writes.

It complements the `snowflake` source rather than replacing it. Openflow moves data and holds no
catalog of its own, so destination table schemas, profiling and column-level lineage come from the
`snowflake` source. Point both at the same account.

## Concept Mapping

| Openflow concept  | DataHub entity                        | Subtype                 |
| ----------------- | ------------------------------------- | ----------------------- |
| Deployment        | Container                             | Openflow Deployment     |
| Runtime           | Container                             | Openflow Runtime        |
| Connector         | DataFlow                              | Openflow Connector      |
| Replicated table  | DataJob (one per table)               | Openflow Connector Sync |
| Destination table | Dataset (on the `snowflake` platform) | Table                   |

A connector is the pipeline, so it maps to a DataFlow; the tables it replicates are the tasks
inside it, so each gets its own DataJob. Keeping one job per table preserves the 1:1 pairing the
connector's configuration states — putting every table's edges on a single job would instead
assert that every source table feeds every destination table.

Openflow's own entities use the `openflow` platform. The destination tables stay on `snowflake` so
they join the warehouse ingestion.
