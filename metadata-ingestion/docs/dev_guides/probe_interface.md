# Adding probe support to a source

The probe interface lets someone — a person at a terminal, or an AI coding assistant — ask a
live source _"what is in you, and what would my recipe actually pick up?"_ before running an
ingestion. This guide explains how it works and how to add it to a connector.

## Why it exists

Writing a recipe is guesswork until you run it. You choose a source type, guess at the config
fields, guess at the `AllowDenyPattern`s, run an ingestion and read the report to find out what
you got. The probe answers those questions up front, and it answers them **the way ingestion
will** — which is the part that makes it useful rather than merely convenient.

Everything prints JSON and every failure has a distinct exit code, so a caller can tell "your
input was wrong" (2) from "I could not reach the source" (3).

## The shape of it

Two things a caller needs, kept deliberately separate:

|                                 | command        | connection? |
| ------------------------------- | -------------- | ----------- |
| **fetch** — what is in here     | `probe run`    | yes         |
| **judge** — what gets picked up | `probe filter` | **no**      |

Splitting them is what keeps both simple. Fetching stops needing to know about filters, and
filtering becomes a pure function over names the caller already has — so a caller can try a
dozen candidate patterns against one listing without touching the source again.

`probe methods` lists what a connector offers, connection-free. It is the discovery surface:
a command's parameters imply the nesting (`columns(schema, table)` sits under `tables(schema)`),
and its docstring is the help text. Nothing is declared twice.

## Everything a connector exposes is a probe method

There is one execution command. A connector adds capability by annotating a
method; `probe run <command>` invokes it and `probe methods` describes it.

```python
class MySource(...):
    @probe_method(name="data_sources")
    def _get_data_sources(self) -> Dict[int, dict]:
        """Warehouse connections this workspace can query. Returns the raw API records."""
```

Parameters become CLI flags (`str`/`int`/`bool`, or `Optional` of those) and the **docstring
becomes the help text**, so a caller discovers capability at runtime. Discovery uses `dir()`, so
annotating the connector's own methods works — a provider can be the source itself.

### Methods that take something dangerous

A catalog query and an API path are still ordinary methods. What makes them safe is that the
method **declares which parameter carries the dangerous value**, and the framework checks it
before invoking:

```python
@probe_method(name="sql", scoped_sql_param="query")
def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
    """Run a read-only catalog query."""
    ...                     # `query` has already been scope-checked

@probe_method(name="api", scoped_path_param="path")
def api(self, path: str) -> object:
    """Fetch one listed read endpoint."""
    ...                     # `path` has already been allowlist-checked
```

Enforcement lives in `probe_methods._enforce_gates`, never in the method — a connector cannot
forget a check it does not perform. Same split as `Filters(...)` on a config field: declare the
fact, let the framework act on it. A declaration naming a parameter that does not exist raises at
decoration time, because a typo would otherwise gate nothing silently.

An earlier design gave these their own CLI commands so that `probe run` had no path to a query
surface at all. That was a stronger _kind_ of guarantee — structural rather than enforced — but it
bought it with two parallel execution paths, and it left `probe methods` an incomplete picture of
what a connector could do. The guarantee is now "the channel is gated by a declaration the
framework enforces," which is worth knowing when reviewing a new `scoped_*` declaration.

A provider supplies what each gate needs: `sql_dialect` (a name sqlglot resolves) for queries, and
`api_allowlist` for paths. A missing `sql_dialect` refuses the query rather than guessing a
grammar. If your connector's dialect name differs from SQLAlchemy's (`postgresql` vs `postgres`),
add it to the map in `sqlalchemy_probe.py`.

### The scope gate

`agent/sql_gate.py` parses with sqlglot and permits only a single `SELECT` whose every table
reference resolves into the dialect's catalog schemas. Two of its rules are not obvious:

- **A relation in a catalog schema is not automatically metadata.** `pg_stat_statements` and
  Snowflake's `ACCOUNT_USAGE.QUERY_HISTORY` carry the literal text of user queries, values in
  `WHERE` clauses included. Those are excluded by name.
- **Vendor-specific functions are refused wholesale.** sqlglot models standard SQL functions as
  their own node types and leaves unmodelled ones as `exp.Anonymous` — and every known way to
  reach data without naming a table (`pg_read_file`, `pg_ls_dir`, `dblink`, `load_file`,
  `SYSTEM$…`, `EXTERNAL_QUERY`) is unmodelled. Refusing the class is fail-closed where a
  denylist of names could never be complete.

`agent/api_gate.py` refuses every method but GET, anything that is not a path on the connector's
own host (absolute URLs, protocol-relative `//host`, `..`, percent-encoded `%2e%2e`), and any path
outside the allowlist. It is **weaker in kind** than the SQL gate and the docs should not imply
parity: there is no parser, so it can only match an allowlist, and whether a listed endpoint
returns metadata or user data is the judgement of whoever listed it.

**Neither is a security boundary.** They narrow what a probe can reach; the enforcement boundary
is the database's own grants. Recommend a read-only role scoped to catalog metadata.

**A passthrough does not replace typed methods.** A raw record leaves the caller to guess which
field a pattern is matched against; for a Mode Space that is the raw `name` with no token
fallback. Fetch generalises; naming and judging do not.

### Providers that must not run `__init__`

If the provider needs connector methods but must not run `__init__` (which typically opens a
connection and emits telemetry), build an uninitialized instance with `__new__` and prime only
the attributes those methods touch. `ModeSource.for_probe()` is the worked example.

A provider that degrades rather than fails exposes a `warnings: List[str]`; `run_probe_method`
reads it back, so an empty result carries its reason.

**Probe output is metadata only** — names, types, constraints, DDL, counts. Never table rows,
column values, or message payloads.

## Making verdicts match ingestion

`probe filter` resolves three things per connector. Two have defaults that are usually right.

**Which field filters this kind.** Resolved by convention from the level's subtype (`Table` →
`table_pattern`). Where the config follows the source's own vocabulary instead, declare it:

```python
collection_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(...)
```

**What string the pattern is matched against.** This is the one that bites. `AllowDenyPattern`
uses a start-anchored `re.match`, and ingestion rarely matches the bare name — MySQL matches
`schema.table`, Postgres `db.schema.table`, Druid the bare name. Get it wrong and `^orders$`
silently matches nothing.

Never re-derive it. The SQL family routes through `SQLCommonConfig.probe_match_target`, which
calls the connector's own `get_identifier` via the shim in `sql_probe.py`; a connector whose
real Source isn't a `SQLAlchemySource` overrides `probe_filter_target` instead (see
`RedshiftConfig`, `UnityCatalogSourceConfig`). A connector whose display name **is** its filter
target — Kafka topics, Mode spaces — needs no hook at all.

Note the shim resolves a _table's_ identifier. Container kinds (Schema, Database) match on the
bare name; asking the shim about a schema would build `analytics..public`.

**Structural exclusions the user's patterns don't express.** `default_schemas()` and
`default_databases()` drop system catalogs whatever the pattern says, and
`probe_schema_verdict_override` lets a connector answer "is this schema allowed" its own way
(Redshift's `match_fully_qualified_names` makes ingestion judge `database.schema`). These run
before the pattern, so a system catalog reports `default_schema` rather than a verdict
ingestion never makes.

## The rules that matter

These have each cost a review round.

**Reuse the connector's fetch; never its policy.** Ingestion degrades on error to salvage
partial metadata; a diagnostic must not, because distinguishing _"nothing here"_ from _"I could
not look"_ is its entire job. When a connector method looks like the right thing to delegate to
but filters internally or swallows errors, reach for the layer beneath it.

**Beware the function whose name matches your intent.** `_get_definitions_map()` sounds like a
definitions fetcher; it is a lossy `{name: source}` cache for template expansion.
`_get_space_name_and_tokens()` sounds like a space lister; it applies `space_pattern` itself, so
delegating to it would make a _denied_ space vanish instead of being reported as excluded. Read
the target before delegating.

**Never construct a `Source`.** `__init__` typically opens connections and emits telemetry. Use
a client, or an uninitialized instance.

This is where the probe departs from `test_connection`, and it is worth knowing why. Most
connectors implement `test_connection` the way a probe wants — Snowflake builds a connection
config and asks it for a connection; Kafka and Unity Catalog delegate to a purpose-built
connection test. The SQLAlchemy family is the exception: `SQLAlchemySource.test_connection`
(`sql/sql_common.py`) calls `cls.create(config_dict, PipelineContext(...))` to borrow one
method, and the cost is visible in the lines above it, which force `stateful_ingestion.enabled
= False` so that merely constructing the object doesn't demand a second connection to DataHub.
That patches one `__init__` side effect; the constructor also emits telemetry and builds a
`ClassificationHandler`, a `DomainRegistry` and a `SqlParsingAggregator`. Don't copy that branch.

**Report degradation, don't hide it.** A 404/403 on one sub-listing should degrade to empty and
say so; auth failures and 5xx should raise. `soft_on_status(403, 404, context=…)` gives you the
split. An empty result **with** warnings means "part of this could not be read" — never "this is
empty".

## Testing expectations

- **The gate, adversarially.** Anything touching `sql_gate` needs attack cases, not happy paths:
  a user table hidden in a CTE, a subquery, a `UNION` branch, a join; an unqualified name; two
  statements; a vendor function in projection position (which names no table at all, so a
  table-based check never sees it). Also test the false-positive side — a legitimate catalog
  query with a trailing semicolon, a recursive CTE, standard aggregates — because a gate that
  refuses real queries is also broken.
- **`execute_sql` stays unannotated.** One assertion per provider.
- **Filter targets.** If the connector has a `get_identifier` equivalent, assert the probe's
  target equals what ingestion computes for the same inputs. `test_sql_filter_target.py` covers
  the SQLAlchemy family, including the Redshift schema override.
- **The degrade path.** A 404 on one sub-listing produces an empty result **and** a warning; auth
  failures and 5xx raise.
- **The connector's existing suites must pass unedited.** A probe adds to a connector; it does
  not change it.
