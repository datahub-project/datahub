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
| Connector         | DataFlow + DataJob                    | Openflow Connector      |
| Replicated table  | DataJob (one per table)               | Openflow Connector Sync |
| Destination table | Dataset (on the `snowflake` platform) | Table                   |

Each replicated table gets its own DataJob so lineage keeps the 1:1 pairing the connector's
configuration states; the connector-level DataJob is an anchor for its properties and ownership
and carries no lineage of its own. Putting every table's edges on one job would instead assert
that every source table feeds every destination table.

Openflow's own entities use the `openflow` platform. The destination tables stay on `snowflake` so
they join the warehouse ingestion.
