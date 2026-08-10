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

|                                 | command                               | connection? |
| ------------------------------- | ------------------------------------- | ----------- |
| **fetch** — what is in here     | `probe sql`, `probe api`, `probe run` | yes         |
| **judge** — what gets picked up | `probe filter`                        | **no**      |

Splitting them is what keeps both simple. Fetching stops needing to know about filters, and
filtering becomes a pure function over names the caller already has — so a caller can try a
dozen candidate patterns against one listing without touching the source again.

`probe methods` lists what a connector offers, connection-free. It is the discovery surface:
a command's parameters imply the nesting (`columns(schema, table)` sits under `tables(schema)`),
and its docstring is the help text. Nothing is declared twice.

## Adding a catalog-query surface (SQL sources)

Implement two members on the connector's probe provider and the rest is free:

```python
class MyMetadataProbe:
    sql_dialect = "postgres"          # a name sqlglot resolves; see below

    def execute_sql(self, query: str, limit: int) -> SqlRows:
        """Run an already-scope-checked query, reading at most `limit` rows."""
        ...
```

Then point the config at it:

```python
class MyConfig(...):
    def build_probe_provider(self) -> ProbeProvider:
        return MyMetadataProbe(self.get_client())
```

Every SQLAlchemy connector already has this through `SqlAlchemyMetadataProbe`, which derives
its dialect from the engine.

**`execute_sql` must never be a `@probe_method`.** Annotating it would put a raw-SQL parameter
on `probe run`, reaching the engine without the scope check — the gate would still exist, just
no longer on the only road. A test pins each provider's `execute_sql` as unannotated; keep it.

### The scope gate

`agent/sql_gate.py` parses with sqlglot and permits only a single `SELECT` whose every table
reference resolves into the dialect's catalog schemas. It runs in the framework, ahead of the
provider, so a connector never receives an unvalidated query.

Two of its rules are not obvious:

- **A relation in a catalog schema is not automatically metadata.** `pg_stat_statements` and
  Snowflake's `ACCOUNT_USAGE.QUERY_HISTORY` carry the literal text of user queries, values in
  `WHERE` clauses included. Those are excluded by name.
- **Vendor-specific functions are refused wholesale.** sqlglot models standard SQL functions as
  their own node types and leaves unmodelled ones as `exp.Anonymous` — and every known way to
  reach data without naming a table (`pg_read_file`, `pg_ls_dir`, `dblink`, `load_file`,
  `SYSTEM$…`, `EXTERNAL_QUERY`) is unmodelled. Refusing the class is fail-closed where a
  denylist of names could never be complete.

Everything fails closed, dialect resolution included: a platform that `get_dialect_str` cannot
map refuses the query outright rather than parsing it against a guessed grammar — which would
mean clearing a query the parser had misread. If your connector's dialect name differs from
SQLAlchemy's (`postgresql` vs `postgres`), add it to the map in `sqlalchemy_probe.py`.

**This check is not a security boundary.** It narrows what a probe query can touch; the
enforcement boundary is the database's own grants. Recommend a read-only role scoped to catalog
metadata, and say so in the connector's docs.

## Adding metadata getters

For sources with no SQL surface — and for anything a query cannot express — annotate a method:

```python
class MySource(...):
    @probe_method(name="data_sources")
    def _get_data_sources(self) -> Dict[int, dict]:
        """Warehouse connections this workspace can query. Returns the raw API records."""
```

Parameters become CLI flags (`str`/`int`/`bool`, or `Optional` of those) and the **docstring
becomes the help text**, so a caller discovers capability at runtime. Discovery uses `dir()`, so
annotating the connector's own methods works — a provider can be the source itself.

If the provider needs connector methods but must not run `__init__` (which typically opens a
connection and emits telemetry), build an uninitialized instance with `__new__` and prime only
the attributes those methods touch. `ModeSource.for_probe()` is the worked example.

A provider that degrades rather than fails exposes a `warnings: List[str]`; `run_probe_method`
reads it back, so an empty result carries its reason.

**Probe output is metadata only** — names, types, constraints, DDL, counts. Never table rows,
column values, or message payloads.

## Adding an API passthrough (non-SQL sources)

The API analogue of the catalog query. A connector opts in by declaring read endpoints as data
and supplying one fetch method:

```python
class MyMetadataProbe:
    api_allowlist = ("GET /spaces", "GET /spaces/{token}/reports")

    def get_json(self, path: str) -> object:
        return self._get_request_json(f"{self.base_uri}{path}")
```

A placeholder matches exactly one path segment. An empty allowlist means nothing is reachable,
so a connector that has not opted in exposes no endpoints. `get_json` must not be a
`@probe_method`, for the same reason `execute_sql` must not.

`agent/api_gate.py` refuses every method but GET, anything that is not a path on the
connector's own host (absolute URLs, protocol-relative `//host`, `..`, percent-encoded
`%2e%2e`), and any path outside the allowlist.

**This gate is weaker in kind than the SQL one, and the docs should not imply parity.** sqlglot
lets `sql_gate` reason about what a query _touches_; a path is opaque, so all the API gate can
do is match an allowlist. Whether a listed endpoint returns metadata or user data is the
judgement of whoever listed it. One dimension is stronger, though — "only GET" is exact, where
"only SELECT" needed CTE and subquery analysis to mean anything.

**A passthrough does not replace getters.** A raw record leaves the caller to guess which field
a pattern is matched against; for a Mode Space that is the raw `name` with no token fallback.
Fetch generalises; naming and judging do not.

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
