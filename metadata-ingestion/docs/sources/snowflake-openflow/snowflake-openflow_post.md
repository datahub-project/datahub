### Capabilities

- **Containers** — one container per Openflow deployment and runtime, nested.
- **Table-level lineage** — derived from each connector's own configuration, so it is observed
  rather than declared. Disable with `include_openflow_lineage: false`.
- **Ownership** — from each object's `OWNER`.
- **External link** — each connector's `DataFlow` links to its NiFi canvas, so "view in Openflow"
  opens the flow itself. The URL comes from `DESCRIBE OPENFLOW CONNECTOR`, which `SHOW` does not
  return, so this costs one extra query per connector; disable with
  `include_connector_external_url: false`.
- **Deletion detection** — via stateful ingestion, using `DELETED_ON` from the `ACCOUNT_USAGE`
  views.

Schema metadata, column-level lineage, profiling and dataset usage are deliberately **not**
supported here; use the `snowflake` source for the destination tables.

#### Supported connector types

Every Openflow connector is catalogued regardless of its type: it becomes a DataFlow under its
runtime, with ownership, subtypes and lineage into its destination Snowflake tables.

Deriving the **upstream** side additionally requires knowing the connector type, because each
upstream platform names datasets at a different tier. Three types are mapped:

| Openflow connector definition | Upstream platform | Upstream dataset name   |
| ----------------------------- | ----------------- | ----------------------- |
| `OPENFLOW_POSTGRES_CDC`       | `postgres`        | `database.schema.table` |
| `OPENFLOW_MYSQL_CDC`          | `mysql`           | `database.table`        |
| `OPENFLOW_SQLSERVER_CDC`      | `mssql`           | `database.schema.table` |

A connector of any other type keeps its destination lineage and raises a warning naming the
unmapped definition. The upstream edge is omitted rather than guessed: a dataset URN built at the
wrong tier is still well-formed, so a guess would point at a dataset that cannot exist and nothing
downstream would report it.

### Limitations

- **Gen 2 connectors only.** Gen 1 Openflow connectors are not Snowflake SQL objects, so this
  source cannot see them — and cannot report how many were omitted. If your account runs Gen 1
  exclusively, the connector inventory will be empty.
- **An empty result may be a permissions problem.** `SHOW OPENFLOW …` is privilege-filtered and
  returns **zero rows** with no error when the role lacks `MONITOR`. The ingestion report raises a
  warning in this case rather than reporting success.
- **Column-level lineage is not available.** Openflow publishes no column-to-column mapping:
  neither a connector's configuration nor the `OPENFLOW_*` `ACCOUNT_USAGE` views carry one, and the
  configuration selects source tables by name or pattern, so lineage is derivable only at table
  grain. Note this is a property of what Openflow exposes, not of the pipeline being non-SQL — the
  `fivetran` source emits column-level lineage without parsing anything, by reading the
  `column_lineage` tables Fivetran maintains in its log schema. Snowflake would need to publish an
  equivalent for Openflow.
- **The external link is undocumented and access-gated.** The canvas URL is only discoverable from
  a `DESCRIBE` column -- no Snowflake documentation page names the endpoint -- so its shape could
  change. Opening it also requires access to the deployment's SPCS ingress, which is separate from
  the Snowflake privileges this source needs; a viewer without it gets an auth error, not a flow.
  A connector known only from the history views (a dropped one, typically) cannot be addressed by
  `DESCRIBE` at all and is emitted without a link.
- **Kafka-source connectors get no upstream lineage.** `OPENFLOW_KAFKA` is deliberately unmapped.
  DataHub names a Kafka dataset by the bare topic, but this source reconstructs an upstream from a
  connector's `jdbc:` Source URL and its schema-qualified table list, and a Kafka connector carries
  neither.
- **Only the `SOURCE_SCHEMA` destination schema strategy is supported.** Prefix, Suffix and Pattern
  strategies exist; a connector using one has its lineage skipped and counted in the report rather
  than guessed.
- **Connectors configured with a table pattern** (rather than explicit table names) cannot have
  their tables enumerated from configuration, so they receive connector-level metadata without
  table lineage.
- **`ACCOUNT_USAGE` views lag.** A newly created runtime can take ~20 minutes to appear. The source
  reconciles `SHOW` against the views and treats a `SHOW`-only object as new, so freshly created
  objects are still ingested.

### Troubleshooting

**The ingestion succeeds but no Openflow objects appear.** Almost always a missing `MONITOR` grant
rather than an empty account, because `SHOW OPENFLOW …` is privilege-filtered and returns zero rows
without an error. Check with `SHOW GRANTS TO ROLE <role>` and `SHOW FUTURE GRANTS IN SCHEMA
<db>.<schema>` — remember FUTURE grants appear only in the latter.

**Lineage points at Snowflake tables that do not exist in DataHub.** `snowflake_platform_instance`
and `snowflake_env` must match your `snowflake` recipe exactly. A mismatch produces well-formed URNs
that resolve to nothing, and nothing reports an error.

**Lineage points at upstream tables that do not exist in DataHub.** The same hazard on the other
side of the edge: `source_platform_instance` and `source_env` must match the recipe that ingests the
system a connector reads from (e.g. your `postgres` recipe). Both default to unset /
this source's `env`, which is correct only when that recipe uses no platform instance.

Upstream table and schema names are emitted exactly as the connector's configuration spells them.
`convert_urns_to_lowercase` applies to the destination Snowflake side only, because `postgres`,
`mysql` and `mssql` all preserve identifier case by default — folding the upstream name would point
the edge at a dataset none of those recipes ever wrote. If you do run one of those recipes with
`convert_urns_to_lowercase: true`, set `source_convert_urns_to_lowercase: true` here to match —
DataHub's MSSQL source actively advises enabling it for lineage, so this is a realistic case
rather than a hypothetical one.

`source_convert_urns_to_lowercase` folds **both** the upstream identifier and
`source_platform_instance`, because the pipeline-level pass that an explicitly-flagged upstream
recipe engages folds the whole URN name, prefix included. Folding only one half would match no
upstream configuration at all.

The destination side deliberately behaves differently, which is worth knowing if you set both.
This source folds the destination identifier itself rather than letting the pipeline-level pass do
it, because that pass rewrites every dataset URN in the stream and would take the upstream ones
with it — but it folds only the identifier, leaving `snowflake_platform_instance` verbatim. The two
halves differ because their defaults differ: an upstream that folds must have been told to, whereas
`SnowflakeIdentifierConfig` folds the destination identifier by default. So an **uppercase**
`snowflake_platform_instance` will not match a `snowflake` recipe that has
`convert_urns_to_lowercase: true` spelled out. Use a lowercase platform instance on both sides and
the question does not arise; the ingestion report warns when it cannot be sure.

**A connector is catalogued but has no lineage.** One of three: its connector type is not in
[Supported connector types](#supported-connector-types), its destination schema strategy is not
`SOURCE_SCHEMA`, or it selects tables by pattern rather than by name. All three are counted in the
ingestion report; only the first also names the unmapped definition in a warning.

**A connector is catalogued but does not appear under its runtime.** `SHOW OPENFLOW CONNECTORS` is
account-wide while runtimes are privilege-filtered, so a connector can name a runtime this run never
saw. The connector is still ingested, just not nested. Every such connector is counted in
`num_connectors_without_runtime_parent`; the ones whose runtime was excluded by `runtime_pattern` are
counted silently, and the rest also raise a warning — grant `MONITOR` on the runtime.

**`GRANT MONITOR ON OPENFLOW CONNECTOR` fails.** Expected — `MONITOR` is not a valid privilege on
that object type. Grant on the parent runtime instead.
