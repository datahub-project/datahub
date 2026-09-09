## Overview

Snowflake Openflow is Snowflake's managed integration service, built on Apache NiFi. This source
catalogs Openflow's own objects — deployments, runtimes and connectors — and emits table-level
lineage from each connector to the Snowflake tables it writes.

It complements the `snowflake` source rather than replacing it. Openflow moves data and holds no
catalog of its own, so destination table schemas, profiling and column-level lineage come from the
`snowflake` source. Point both at the same account.

## Concept Mapping

| Openflow concept  | DataHub entity                        | Subtype             |
| ----------------- | ------------------------------------- | ------------------- |
| Deployment        | Container                             | Openflow Deployment |
| Runtime           | Container                             | Openflow Runtime    |
| Connector         | DataFlow + DataJob                    | Openflow Connector  |
| Destination table | Dataset (on the `snowflake` platform) | Table               |

Openflow's own entities use the `openflow` platform. The destination tables stay on `snowflake` so
they join the warehouse ingestion.
