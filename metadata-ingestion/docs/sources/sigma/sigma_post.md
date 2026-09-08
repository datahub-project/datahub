### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connection record overrides (`connection_to_platform_map`)

All four warehouse lineage paths — DM element → warehouse table, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS — resolve the target DataHub platform and URN
coordinates from the Sigma connection record. `connection_to_platform_map` lets you override
those coordinates per connection.

**env / platform_instance / convert_urns_to_lowercase**

For multi-environment or multi-instance setups, specify the exact env and platform_instance per
Sigma connectionId so emitted lineage edges point to the correct warehouse connector run:

```yml
connection_to_platform_map:
  # Key is the Sigma connectionId (UUID from /v2/connections).
  "4b39cdcd-5a58-4ff6-af0d-8409ff880a23":
    env: PROD
    platform_instance: prod-snowflake
    # Set to false only if the Snowflake connector was run with
    # convert_urns_to_lowercase: false (non-default).
    convert_urns_to_lowercase: true
```

**Warehouses that omit database or schema from the Sigma connection record** (e.g. Redshift):
Sigma's `/v2/connections` API does not return `database` or `schema` fields for all warehouse
types. When those fields are absent, lineage URNs may be under-qualified and will not match
what your warehouse connector emitted. Use `default_database` and `default_schema` to supply
the missing values:

```yml
connection_to_platform_map:
  "a1b2c3d4-0000-0000-0000-000000000001":
    env: PROD
    default_database: my_redshift_db # expands `schema.table` → `my_redshift_db.schema.table`
    default_schema: public # expands bare `table` → `public.table` (SQL parser only)
```

`default_database` applies to all four lineage paths (DM element, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS). `default_schema` is consumed only by the
SQL parser (DM customSQL and workbook customSQL paths) — the entity-level BFS and DM element
paths derive the schema from the `/files` path and do not use this field. The `env` and
`platform_instance` fields are also consumed by the customSQL parsers, so URNs minted from
customSQL definitions match the warehouse connector's env/instance for that connection.

#### Data Model customSQL element lineage

Data Model elements backed by a customSQL source emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` automatically — no additional configuration is required beyond a valid connection in the Sigma connection registry for most platforms. **Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`, set `default_database` and `default_schema` in `connection_to_platform_map` — see [Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

For elements with explicit column lists in their SQL (`SELECT col_a, col_b FROM ...`), column lineage is derived directly from the SQL by the parser (confidence score 0.2). For elements using `SELECT *`, column lineage is inferred from Sigma's formula metadata (`[Custom SQL/COL]` refs on each element column); these entries carry a confidence score of 0.1 — lower than SQL-parsed lineage — because they rely on formula-derived inference rather than direct SQL analysis.

The following report counters are available for operational visibility:

| Counter                                     | Meaning                                                                                        |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `dm_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `dm_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `dm_customsql_skipped`                      | Elements skipped before parsing (missing definition, unknown connection, unsupported platform) |
| `dm_customsql_parse_failed`                 | Definitions the SQL parser could not interpret (syntax errors, unsupported features, etc.)     |
| `dm_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `dm_customsql_column_lineage_emitted`       | Elements with at least one column lineage entry emitted                                        |
| `dm_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook customSQL chart lineage

When `extract_lineage: true` (default), workbook chart elements whose data source is a customSQL definition
emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` via the SQL parser — no additional
configuration is required beyond a valid connection in the Sigma connection registry for most platforms.
**Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`,
set `default_database` and `default_schema` in `connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

The connector reads the workbook-level lineage graph (`/v2/workbooks/{id}/lineage`) to find `type=customSQL`
entries, parses each SQL definition, and registers the results with the `SqlParsingAggregator`.
Column-level lineage is emitted for chart columns whose formula resolves to a named SQL column
(`[CustomSQLName/col]` pattern). Columns using `SELECT *` sources carry lower-confidence (0.1) inferred lineage.

The following report counters are available for operational visibility:

| Counter                                           | Meaning                                                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `workbook_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `workbook_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `workbook_customsql_skipped`                      | Entries skipped before parsing (missing definition, unknown connection, unsupported platform)  |
| `workbook_customsql_parse_failed`                 | Definitions the SQL parser could not interpret                                                 |
| `workbook_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `workbook_customsql_column_lineage_emitted`       | Charts with at least one column lineage entry emitted                                          |
| `workbook_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook chart entity-level warehouse upstream

When `extract_lineage: true` (default), workbook chart elements that pull **directly** from a
warehouse table (no Data Model, no customSQL, no Sigma Dataset in between) emit an entity-level
`chartInfo.inputs` edge to the warehouse Dataset.

Column-level lineage for these charts is handled separately by the
[chart inputFields warehouse qualification](#workbook-chart-inputfields-warehouse-column-level-qualification)
path; this feature adds the missing entity-level edge.

No additional configuration is required for most platforms. For Redshift connections where the
Sigma connection record omits `database`/`schema`, set `default_database` in
`connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

| Counter                                | Meaning                                                                                                           |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `chart_warehouse_upstream_emitted`     | Entity-level chart→warehouse edges emitted (post-dedup)                                                           |
| `chart_warehouse_table_name_unmatched` | Table not found in workbook warehouse index; edge not emitted                                                     |
| `chart_warehouse_table_node_skipped`   | Lineage node missing `name` field or has unexpected ID format; skipped                                            |
| `chart_warehouse_table_name_ambiguous` | Table name matched multiple warehouse URNs; edge skipped — set `default_database` in `connection_to_platform_map` |

#### Chart formula refs that reach through a join

Sigma writes a column reached through a join as `[JoinElement/SourceElement/Column]`. Read
at the first slash that names a column `SourceElement/Column`, which the upstream does not
have, so the `InputField` produced pointed at a field that does not exist. The connector
now tries every split of such a ref and accepts only one whose upstream actually has the
column, including the case where the middle segment is a table joined in inside a Data
Model (resolved among that model's own elements).

> **Some `InputFields` that previously named an upstream now self-reference.** When no
> split validates, the ref is dropped rather than emitted at the first slash, because the
> old reading produced a dangling `schemaFieldUrn`. The column still appears in the chart's
> column list, pointing at itself. `chart_join_chain_dangling_suppressed` counts these, and
> `chart_input_fields_self_ref_unresolved_refs` rises by the same amount that
> `chart_input_fields_resolved` falls.

| Counter                                | Meaning                                                                 |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `chart_join_chain_resolved`            | Multi-segment refs resolved and schema-validated                        |
| `chart_join_chain_sibling_resolved`    | Subset resolved via a sibling element of the first segment's Data Model |
| `chart_join_chain_unresolved`          | No split validated; the column self-references                          |
| `chart_join_chain_dangling_suppressed` | Refs dropped rather than emitted as a dangling field                    |
| `chart_join_chain_sibling_ambiguous`   | Two sibling elements share the middle segment's name; refused           |

#### Workbook chart inputFields warehouse column-level qualification

When `extract_lineage: true` (default), the connector qualifies chart column `InputFields` to
warehouse Dataset URNs. For each chart column whose formula references a warehouse table (e.g.,
`[TABLE/col]`), the connector resolves the short table name to a fully-qualified warehouse Dataset
URN via a two-level index: first the per-element SQL-parser index, then the workbook-level index
from `/v2/workbooks/{id}/lineage`. The resolved URN is written into `schemaFieldUrn` on each
`InputField` entry.

| Counter                                                     | Meaning                                                              |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `chart_input_fields_warehouse_qualified`                    | Individual column fields successfully qualified to a warehouse URN   |
| `chart_input_fields_warehouse_qualified_via_workbook_index` | Subset qualified via the workbook-level index (not per-element SQL)  |
| `chart_input_fields_warehouse_index_lookup_failed`          | Workbook-level lineage fetch failed; column qualification incomplete |
| `chart_input_fields_warehouse_table_lookup_failed`          | `/files/{inodeId}` call failed for a workbook-level table entry      |
| `chart_input_fields_warehouse_path_unparseable`             | `/files` path did not match expected format                          |
| `chart_input_fields_warehouse_unknown_connection`           | ConnectionId not in registry or platform unmappable                  |

#### Data Model element -> warehouse table lineage

When `ingest_data_models: true` and `extract_lineage: true` (both default), the connector also emits entity-level `UpstreamLineage` from each Sigma Data Model element to the warehouse table it is sourced from.
Resolution uses Sigma's `/v2/dataModels/{id}/lineage` (`type=table` entries) and `/v2/files/{inodeId}` to construct the fully-qualified `<DB>/<SCHEMA>/<TABLE>` identifier from the path and table name fields (path = `Connection Root/<DB>/<SCHEMA>` for most platforms; `Connection Root/<SCHEMA>` for Redshift), then maps the Sigma connection to a DataHub platform via the connection registry.

**Supported platforms**: All Sigma connection types in `SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP` (Snowflake, BigQuery, Redshift, Databricks, Postgres, MySQL, Athena, Spark, Trino, Presto, Synapse/MSSQL).
Identifier casing is preserved as Sigma reports it, which matches the warehouse catalog for most platforms.
Snowflake is the only platform that requires a case bridge (Snowflake's catalog uses uppercase identifiers, but the DataHub Snowflake connector lowercases them by default).

**Matching URNs to your warehouse connector**: The emitted URNs use the Sigma recipe's `env` and
`platform_instance=None` by default. For multi-environment or multi-instance setups, or for
Redshift connections where the Sigma connection record omits `database`/`schema`, see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

**Warehouse column names are confirmed against DataHub where possible.** Sigma reports a
warehouse column by its display name (`Order Ref Id`), and where the `columnId` does not carry the
native name the connector has to derive it (`ORDER_REF_ID`) — a convention, not a fact, and this
connector holds no warehouse schema of its own to check it against. So when the DataHub graph is
reachable it reads that table's `schemaMetadata` (once per table, cached for the run) and emits the
field name the warehouse connector itself recorded, scoring it as high as a `columnId`-derived
name. `warehouse_column_verified_against_graph` counts those.

Where DataHub holds no schema for the table — no graph, or the warehouse connector has not ingested
it yet — the derived name stands and is counted under `warehouse_column_unverifiable_no_schema`.
That is a different risk, not the same one: the dataset is an un-ingested stub, so there is no
schema for a wrong name to contradict, and the derived name is the only signal available. The case
worth watching is `warehouse_column_absent_from_graph_schema` — DataHub **has** the schema and
neither the display name nor the derived name matches any field in it, which is the one situation
where the derived name is provably a dangling field reference. The edge is still emitted at the
reduced confidence so the information is not lost, but a large value there means the convention
does not hold on your warehouse.

**Counters to monitor** (visible in the ingestion report):

| Counter                                       | Meaning                                             |
| --------------------------------------------- | --------------------------------------------------- |
| `dm_element_warehouse_upstream_emitted`       | Warehouse lineage edges successfully emitted        |
| `dm_element_warehouse_unknown_connection`     | ConnectionId not in registry or platform unmappable |
| `dm_element_warehouse_table_lookup_failed`    | `/files/{inodeId}` call failed                      |
| `dm_element_warehouse_path_unparseable`       | `/files` path did not match expected format         |
| `dm_element_warehouse_table_entry_incomplete` | Lineage entry missing inodeId or connectionId       |

##### Chart source platform mapping

`chart_sources_platform_mapping` is the legacy fallback for the workbook-chart SQL parser path.
It fires whenever a workbook element exposes an `element.query` (regardless of whether the element
is backed by a Sigma Dataset, a DM element, customSQL, or inline SQL). The SQL-bearing endpoint
does not return a `connectionId`, so the platform, env, and default database/schema cannot be
auto-resolved. You declare them explicitly, scoped to a workbook path prefix or the `"*"` wildcard.

**Prefer `connection_to_platform_map`** for warehouse connections (Snowflake, Redshift, BigQuery,
etc.) — it auto-resolves the platform from the connection record and covers all lineage paths that
have a `connectionId` on hand (DM warehouse table lineage, DM and workbook customSQL parsing,
chart inputFields qualification). Use `chart_sources_platform_mapping` only when the chart's SQL
parser path fires and you cannot reach the connection via `connection_to_platform_map`.

##### Example - For just one specific chart's external upstream data sources

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name/chart_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name/chart_name_2":
    data_source_platform: postgres
    platform_instance: cloud_instance
    env: DEV
```

##### Example - For all charts within one specific workbook

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name_2":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - For all workbooks charts within one specific workspace

```yml
chart_sources_platform_mapping:
  "workspace_name":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - All workbooks use the same connection

```yml
chart_sources_platform_mapping:
  "*":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Column-level lineage coverage for Data Model elements

Column-level lineage for Data Model elements is derived from the column formulas returned by
Sigma's `/v2/dataModels/{id}/columns` endpoint. A column gets one upstream edge per source
its formula names, so coverage follows the formula rather than the element's table-level
upstreams.

The practical consequence shows up on joins. A join's output column carries a formula
naming only one side, so `/columns` alone can only ever produce an edge to that side. The
predicate itself is exposed on `/v2/dataModels/{id}/spec`, which the connector now reads
once per Data Model: where a predicate equates a column an edge already reaches with a
column on the other side, the other side is emitted as an additional upstream. Because a
predicate is an equality rather than a value copy, those edges carry a lower
`confidenceScore` than formula-derived ones (0.7 for an inner join, 0.6 for an outer join,
where the equality holds only on the rows the join matched), so consumers wanting only
value-propagation lineage can filter them out.

A predicate is applied only to elements that read **through** its join — the join element
must be in the element's own upstream chain. Two elements can reference the same key column
while only one of them flows through the join that constrains it, and expanding the other
would assert an equality its data path never applies.
`data_model_join_key_out_of_join_path` counts predicates skipped for this reason.

> **Confidence filtering does not remove the table-level edge.** When a join partner is a
> Data Model element the chart or dataset did not already depend on, that element is also
> added to `upstreamLineage.upstreams`. `Upstream` has no `confidenceScore`, so a consumer
> filtering the 0.6/0.7 column edges still keeps the table-level edge those column edges
> introduced. Set `extract_data_model_spec_lineage: false` to suppress both.

A join's two inputs may both live in **other** Data Models — a shared mapping element joined
into a model that owns neither side. Sigma sends a `dataModelId` on each such side, which is
what pins the element (element ids are not unique across models); a side that resolves to
more than one candidate model is refused rather than guessed at, and counted under
`data_model_join_key_foreign_ambiguous`.

Unions have the same blind spot as joins and the same fix. A `union` element's output column
carries a formula naming at most one branch, so every other branch is invisible from
`/columns`. `/spec` states the branch pairing explicitly, and those edges score **1.0** — a
union stacks rows, so the output column _is_ each branch's column rather than a value derived
from one. `data_model_element_fgl_union_resolved` counts them; if
`data_model_union_branch_index_out_of_range` is non-zero, Sigma changed the shape of the
descriptor and the pairing should not be trusted.

The `/spec` call needs the API token's data model read scope. Without it the call fails,
one warning is reported for the run, `data_model_spec_fetch_failed` counts the affected
models, and join keys are simply absent — everything else still ingests. If
`data_model_join_elements_unreadable` is non-zero, Sigma's join descriptor did not match
the shape the parser reads; run with `--debug` and look for `DM SPEC JOIN` lines, which log
the descriptor's structure (key names and types only, never values).

#### Failed API calls

`api_call_failures_by_status` counts every Sigma API call that failed, keyed by HTTP status
(or by exception class when there was no response). Each one also appears as a
`Sigma API call failed` warning naming the resource. Read this first when lineage looks thin:
a run with hundreds of 404s or 409s is missing input, not mis-resolving it. Failures that the
calling code already reports in more detail — a pagination abort, for instance — are counted
here but not warned about twice.

#### Reading the chart InputFields counters

Every chart column lands in exactly one bucket, and the fallback bucket is split by cause —
these are not interchangeable:

| Counter                                       | Meaning                                                                             |
| --------------------------------------------- | ----------------------------------------------------------------------------------- |
| `chart_input_fields_resolved`                 | a formula ref resolved to an upstream column                                        |
| `chart_input_fields_self_ref_unresolved_refs` | a formula existed and none of its refs resolved — see `chart_ref_miss_reasons`      |
| `chart_input_fields_formulas_not_fetched`     | **our** `/columns` call for that workbook aborted, so no formula was ever retrieved |
| `chart_input_fields_self_ref_no_formula`      | Sigma genuinely reported no formula for the column                                  |

Sigma's per-element `/lineage` does not declare every element a formula reaches, so
`resolve_chart_refs_by_element_name` (default **`false`**) enables two last-resort steps when
nothing else matches: the source name is matched against the **other elements of the same
workbook**, then against the elements of the **Data Models that workbook loads**.

This is the only resolution step that infers from a _name_ rather than from lineage Sigma
stated, which is why it is opt-in. `InputFields` carry no `confidenceScore`, so an inferred
edge cannot be marked as such and is indistinguishable from a declared one — a tenant whose
elements carry generic names should leave it off. Where it does run, three conditions all
have to hold: the search never leaves the workbook's own Data Models, a name matching more
than one element is refused rather than picked, and the element must have the referenced
column. `chart_ref_workbook_name_*` and `chart_ref_scoped_name_*` report resolutions and each
kind of refusal, so you can judge on your own data whether the inference earns its place.

`chart_ref_miss_reasons` breaks the unresolved bucket down by the resolution step that gave
up. Two of its keys matter most when judging whether a gap is fixable:
`unknown_source_but_name_exists_in_another_data_model` means the name exists in the run but
was not among the upstreams offered for that chart — a scope problem, addressable here;
`unknown_source_absent_from_entire_run` means the run never saw that element at all, because
it was filtered, its `/lineage` returned an error, or it lives outside the ingested
workspaces.

#### Pivot tables and input tables

`pivot-table` and `input-table` workbook elements are ingested as Charts alongside `table`
and `visualization`. They hold real columns that other elements' formulas reference, so
excluding them left those references permanently unresolvable.

> **This emits chart entities that earlier versions did not.** On one tenant (2026-09) it added
> roughly 1,200 charts. It also costs two extra API calls per newly-admitted element. Set
> `ingest_pivot_and_input_tables: false` to keep the previous entity set.

Similarly, `extract_data_model_spec_lineage` (default `true`) controls the one extra
`/dataModels/{id}/spec` call per Data Model. It governs **both** lineages that document
provides — join keys (0.7/0.6) and union branches (1.0) — so turning it off drops both, not
just the join-key edges its name might suggest.

A Data Model can reference a warehouse table that `/v2/files/{urlId}` cannot resolve for
the ingestion credential. Those columns receive no warehouse column lineage and are counted
under `dm_element_warehouse_url_id_unresolvable`; the API does not say whether the file was
deleted or is simply outside what the token can see, so check the credential's access
before assuming the reference is stale.

Note that a table absent from `/v2/files?typeFilters=table` may still resolve through a
direct `/v2/files/{urlId}` call — on one tenant (2026-09) the listing omitted 52 tables that the
direct lookup returned in full. The connector uses the direct call for this reason.

Two report counters mark data that never arrived, and should be read before treating a
model's missing lineage as a resolution failure: `data_model_columns_fetch_partial` counts
Data Models whose `/columns` pagination aborted (that endpoint is the only source of
formulas and column ids), and `column_formulas_fetch_partial` counts workbooks whose
`/columns` call aborted, leaving their chart columns with self-referential input fields.
`pagination_aborted` gives the run-wide total.

Columns no formula reference resolves for — a plain pass-through, which Sigma returns with
an empty formula, a constant, or a formula using only parameters — still get column-level
lineage to a warehouse table when their `columnId` identifies the warehouse column. Where
it does not, the ingestion report separates the two outcomes:
`data_model_element_fgl_no_ref_warehouse_unresolved` counts columns that named a warehouse
column but could not be resolved to one, which is worth investigating, while
`data_model_element_fgl_no_ref_unresolved` counts pass-throughs from another Data Model
element or Sigma Dataset, which carry no warehouse identity to resolve and are expected.

When a formula does name an upstream element but that element's column list came back
empty, the edge is dropped under `data_model_element_fgl_upstream_schema_unavailable` (a
sibling in the same Data Model) or
`data_model_element_fgl_cross_dm_upstream_schema_unavailable` (an element in another Data
Model), rather than under `data_model_element_fgl_dropped_unknown_upstream_column`, which
means the column genuinely is not in the upstream's schema. A `Sigma paginated endpoint
aborted` warning naming that Data Model confirms a failed fetch; its absence means the
upstream element really has no columns.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
