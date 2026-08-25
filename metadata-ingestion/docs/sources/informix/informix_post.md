### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required. This module:

- Emits a database → owner (schema) container hierarchy, with tables and views as datasets underneath.
- Emits schema fields with native types (including length, e.g. `VARCHAR(100)`), nullability, and primary-key flags.
- Extracts foreign-key relationships from `sysconstraints` / `sysreferences`, for tables (`include_foreign_keys`).
- Extracts table- and column-level lineage for views by parsing `sysviews.viewtext` (`include_view_lineage`).
- Emits `viewProperties` (the stored view SQL) for every view whose definition is readable.
- Emits approximate row counts from `systables.nrows`, for tables (`include_row_counts`).
- Assigns ownership from `systables.owner` (`include_ownership`).
- Supports stateful ingestion with stale-entity (deletion) detection.

#### Ownership

Ownership is taken from `systables.owner`, which Informix populates with the database
user that created the object. Each schema, table and view is assigned that user as a
`DATAOWNER`.

Two things to be aware of before relying on this:

- Informix records a **database account**, not a person or team. Objects created by an
  administrative account all come back owned by that account (commonly `informix`), so
  the resulting owner is an identity in the DataHub sense but not necessarily a useful
  point of contact.
- The owner name is also the schema name — in Informix the two are the same concept —
  so a schema container is owned by the user it is named after.

Set `include_ownership: false` to skip emitting it.

### Limitations

This module does not support:

- **Column profiling** — no row sampling, null counts, or other column-level statistics; only approximate row counts from `systables.nrows` are emitted.
- **Stored procedures** — SPL routines are not ingested as DataJobs.
- **Usage / query-log lineage** — view lineage is derived only from parsing view SQL definitions, not from query logs or runtime usage.

#### View lineage and Informix-specific SQL

View lineage is produced by parsing `sysviews.viewtext` with sqlglot. sqlglot has no
Informix dialect, so the `postgres` dialect is used: Informix normalizes stored view
text into a qualified, aliased, comma-join form that `postgres` parses correctly for
the common case.

Views whose stored text retains Informix-specific syntax — `MATCHES` / `NOT MATCHES`,
`FIRST` / `SKIP`, native `OUTER` joins, or `DATETIME ... YEAR TO DAY` — will not parse
and get no lineage. This is per-view and non-fatal: the rest of the run is unaffected,
and each failure is counted as `view_lineage_failures` in the ingestion report.

A view can also resolve at the table level while its column lineage fails to parse. In
that case table-level lineage is still emitted, the shortfall is reported as a warning,
and it is counted as `view_column_lineage_failures`.

#### Composite foreign keys

Informix's catalog exposes a constraint's child and parent key columns as two
independent 16-slot `sysindexes` column lists, so a composite foreign key comes back as
a cross product rather than as ordered column pairs. Single-column foreign keys are
always exact. For composite keys the columns are paired best-effort and a warning is
reported, since the catalog does not record the pairing order.

If the two lists come back with different lengths — which happens when a constraint is
backed by a wider pre-existing index — the pairing is ambiguous, so that constraint is
skipped rather than emitted misaligned. Skipped constraints are counted as
`foreign_keys_dropped_mismatched` in the ingestion report.

#### Extended type mapping

`syscolumns.coltype` cannot identify an extended type on its own — `LVARCHAR` is type 40, and
`BOOLEAN`, `BLOB` and `CLOB` all share type 41 — so the column's `extended_id` is resolved
against `sysxtdtypes` to recover the real type name.

`LVARCHAR`, `BOOLEAN`, `BLOB` and `CLOB` map to their DataHub equivalents. A `DISTINCT` type
reports its own name (for example `MONEY_USD`) and takes its DataHub type from the built-in it
was defined over — read from `coltype` for an ordinary built-in, and from `sysxtdtypes.source`
when it was defined over `LVARCHAR`, `BOOLEAN`, `BLOB` or `CLOB`, which `coltype` cannot express.
A `DISTINCT` type defined over another `DISTINCT` type maps to a null type. A named `ROW` type
maps to a record.

User-defined opaque types (`JSON`, `BSON`, time series, and spatial types such as
`ST_Geometry`) have no DataHub equivalent and still map to a null type, but the native type name
is reported rather than a placeholder. Each one is counted as a warning in the ingestion report.

### Troubleshooting

If ingestion fails, first confirm the JDBC driver is resolvable (see Prerequisites) and that the connecting user has `SELECT` on the system catalog tables. Then review ingestion logs for connection or query errors.
