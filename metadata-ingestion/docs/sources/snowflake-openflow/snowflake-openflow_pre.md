### Overview

This source reads Openflow's object inventory over SQL and derives lineage from each connector's own
configuration. It requires a Snowflake user and role, and Openflow **Gen 2** connectors.

### Prerequisites

Openflow object visibility is granted **per object**. A role holding nothing on a runtime sees zero
rows and no error, so grant before you ingest.

#### For all capabilities

```sql
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>;
GRANT USAGE ON DATABASE <db> TO ROLE <role>;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE <role>;

-- The Openflow ACCOUNT_USAGE views. Usually already held as part of a
-- standard ACCOUNT_USAGE grant set.
GRANT SELECT ON VIEW SNOWFLAKE.ACCOUNT_USAGE.OPENFLOW_DEPLOYMENT_HISTORY TO ROLE <role>;
GRANT SELECT ON VIEW SNOWFLAKE.ACCOUNT_USAGE.OPENFLOW_RUNTIME_HISTORY    TO ROLE <role>;
GRANT SELECT ON VIEW SNOWFLAKE.ACCOUNT_USAGE.OPENFLOW_CONNECTOR_HISTORY  TO ROLE <role>;
```

#### Containers — deployments and runtimes

`MONITOR` alone is sufficient. No `USAGE` or `OPERATE` is needed, and a read-only metadata role
should not hold `OPERATE`.

```sql
GRANT MONITOR ON OPENFLOW DEPLOYMENT "<deployment>" TO ROLE <role>;
GRANT MONITOR ON OPENFLOW RUNTIME <db>.<schema>."<runtime>" TO ROLE <role>;

-- Scale it. ALL and FUTURE are COMPLEMENTS, not alternatives: ALL covers what
-- exists now, FUTURE only what is created later. Grant both, or a recipe with
-- only FUTURE silently misses every pre-existing runtime.
GRANT MONITOR ON ALL    OPENFLOW RUNTIMES IN SCHEMA <db>.<schema> TO ROLE <role>;
GRANT MONITOR ON FUTURE OPENFLOW RUNTIMES IN SCHEMA <db>.<schema> TO ROLE <role>;
```

FUTURE grants do **not** appear in `SHOW GRANTS TO ROLE`; check them with
`SHOW FUTURE GRANTS IN SCHEMA <db>.<schema>`.

#### Connectors and lineage

Connectors need no grant of their own — visibility inherits from the parent runtime. Note that
`GRANT MONITOR ON OPENFLOW CONNECTOR` is rejected by Snowflake: `MONITOR` is not a valid privilege
on that object type.

Lineage additionally reads each connector's `config.json` from its version stage, which the runtime
grant above already permits.
