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

## What you implement

**If your source is in the SQLAlchemy family, nothing.** `SQLCommonConfig` already supplies the
whole contract, so a connector inheriting it gets ten probe commands the moment it registers:
the listings `containers`, `tables`, `views`; the per-object `columns`, `foreign_keys`, `indexes`,
`primary_key`, `table_comment`, `view_definition`; and `sql`. Check with `datahub recipe probe methods --recipe r.yml` before writing anything. The
rest of this section is for a source that is not SQLAlchemy-backed, or one whose verdicts come out
wrong.

The hooks are resolved by name, not declared on a base class, so a method with the right name and
signature is picked up and a misspelled one is simply never called. That matters differently in the
two halves below: get the **required** hook wrong and the source reports "no probe methods", which
you will notice immediately; get a **verdict** hook wrong and the probe silently falls back to a
default and reports a verdict your ingestion does not make. `test_probe_contract.py` catches a
misspelled `probe_*` hook for you; it cannot catch a misspelled `default_schemas`.

### Required: one hook, one provider class

```python
class MySourceConfig(ConfigModel):
    # The config's ONLY statement about its provider. `probe methods` describes
    # this class and `probe run` builds it, so the two cannot disagree.
    @classmethod
    def probe_provider_class(cls) -> type:
        from datahub.ingestion.source.mysource.probe import MyMetadataProbe

        return MyMetadataProbe


class MyMetadataProbe:
    def __init__(self, client: MyClient) -> None:
        self._client = client

    # How the framework builds you. Owning construction here is what keeps the
    # config down to one hook.
    @classmethod
    def for_config(cls, config: MySourceConfig) -> "MyMetadataProbe":
        return cls(config.get_client())

    def __enter__(self) -> "MyMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    @probe_method(kind=DatasetSubTypes.TABLE, row_limit_param="limit")
    def tables(self, limit: int = 200) -> List[str]:
        """Tables this workspace exposes, including ones table_pattern would
        exclude -- a denied table is reported, not hidden. Metadata only."""
        return self._client.list_tables()[:limit]
```

That is a working probe, and for a non-SQL source it is usually the whole of it: Kafka and Mode
implement exactly this one hook and nothing else in this guide. Everything below is conditional.

### The provider

| Member                         | When you need it                                                                              |
| ------------------------------ | --------------------------------------------------------------------------------------------- |
| `__enter__` / `__exit__`       | always — it is the `ProbeProvider` protocol, and `__exit__` is where the connection closes    |
| at least one `@probe_method`   | always                                                                                        |
| `sql_dialect: str`             | if any method declares `scoped_sql_param` — a name sqlglot resolves                           |
| `api_allowlist: Sequence[str]` | if any method declares `scoped_path_param` — `("GET /spaces", "GET /spaces/{token}/reports")` |
| `warnings: List[str]`          | if a listing degrades instead of failing; `run_probe_method` reads it back                    |

### `@probe_method` options

| Option              | Effect                                                                                                                                                                                    |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`              | command name; defaults to the method name                                                                                                                                                 |
| `kind`              | the DataHub subtype the returned names are, so `probe filter` picks the right `*_pattern` without the caller guessing a string. Omit only when the caller chooses what comes back (`sql`) |
| `scoped_sql_param`  | names the parameter carrying raw SQL; the framework scope-checks it first                                                                                                                 |
| `scoped_path_param` | names the parameter carrying an API path; allowlist-checked first                                                                                                                         |
| `row_limit_param`   | names the parameter bounding the result; clamped to `1..MAX_PROBE_ITEMS` before the fetch                                                                                                 |
| `parent_params`     | names the parameters identifying the container these names live under; the result reports their values, so a caller need not restate them as `--parent`                                   |

Parameters must be annotated `str`, `int` or `bool` (or `Optional` of those) and the docstring is
required — it is the help text the agent reads.

### Optional: making verdicts match ingestion

`probe filter` resolves a verdict in this order. Every step has a default that is right for most
connectors, so implement a hook only when the default gives the wrong answer.

| Step | What it does                                                                                                                                                    | Hook to override it                                                                                                                                         |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | find the field that filters this kind — by convention from the subtype (`Table` → `table_pattern`)                                                              | `Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)]` on the field                                                                                  |
| 2    | decide the string the pattern is matched against — bare name for container kinds, bare name (with a warning) when no parent was given, otherwise ask the config | `probe_match_target(self, ctx: ClassifyContext) -> str`                                                                                                     |
| 3    | apply exclusions the user's pattern does not express, before the pattern                                                                                        | `default_databases()` / `default_schemas()` (classmethods returning `FrozenSet[str]`), `probe_schema_verdict_override(self, schema: str) -> Optional[bool]` |
| 4    | match the pattern against the target                                                                                                                            | —                                                                                                                                                           |

**Step 1 is the one you are most likely to need**, and for many connectors the only one: declare
`Filters` whenever your config field follows the source's own vocabulary (`collection_pattern`,
`object_pattern`) rather than DataHub's subtype name. A connector whose display name **is** its
filter target — Kafka topics, Mode spaces — needs nothing here at all.

Be aware how narrow the rest is. Step 3 only runs for `Schema` and `Database` kinds, so a source
with neither (Mode's Space/Report/Query, Kafka's Topic) can never reach it. And step 2's hook has
exactly one implementor in the tree — `SQLCommonConfig` — because the SQL family is where display
identity and addressing identity come apart. Implement it if your source filters on a qualified
identifier; otherwise the default, the bare name, is already right.

Copy the signatures exactly. `probe_schema_verdict_override` is invoked as `override(schema=name)`,
so the parameter name is part of the contract — renaming it to `schema_name` raises at probe time,
not at import.

### The SQL family's listings come from the Inspector, not from a query

`containers()`, `tables(schema)` and `views(schema)` on `SqlAlchemyMetadataProbe` go
through SQLAlchemy's Inspector -- `get_schema_names`, `get_table_names`,
`get_view_names` -- which is what ingestion itself enumerates through. That matters
beyond convenience:

- **`tables` and `views` are separate, as the two patterns are.** A catalog query
  against `information_schema.tables` returns both kinds in one result set, so judging
  that listing as tables hands a view a verdict from `table_pattern` when ingestion
  would have used `view_pattern`.
- **The parent travels with the result.** `tables(schema)` declares
  `parent_params=("schema",)`, so the result reports `parent_path=["analytics"]` and
  `probe filter` needs no `--parent` from the caller. Threading it by hand is how it
  goes missing, and for a SQL source a missing container flips the verdict.
- **They work where `sql` cannot.** sqlglot has no dialect for DB2 or Vertica, so the
  gate refuses every query on them; before these listings existed, those two probes
  could enumerate nothing at all.

`containers` declares no `kind`, because the same Inspector call means different things
per tier: three-tier sources return schemas filtered by `schema_pattern`, two-tier ones
return databases filtered by `database_pattern`. The config states which via
`probe_container_kind()`, and the result carries the resolved value.

### Declaring what your dialect's catalog is

`probe sql` permits only reads of catalog metadata, and **which relations those are is your
connector's declaration, not the framework's guess.** Override `probe_catalog_scope()` on your
config:

```python
@classmethod
def probe_catalog_scope(cls) -> CatalogScope:
    return CatalogScope(
        relations=frozenset({"sys.tables", "sys.columns", "sys.objects"}),
    )
```

The default is `information_schema` and nothing else — right for the standard dialects, and safe
for the rest. It used to be a table inside `sql_gate`, and that table was wrong: Oracle and Teradata
have no `information_schema` at all (their catalogs are `DBA_*`/`ALL_*` and `DBC.*`), so both
advertised a `sql` command whose every legitimate query was refused.

**Name relations, not whole schemas** — for anything other than `information_schema`. A vendor
catalog schema is almost never wholly metadata, and our own ingestion code is the proof: it reads
`system.query_log` on ClickHouse, `DBC.QryLogV` on Teradata and `sys.dm_exec_cached_plans` on
MSSQL. Those carry executed SQL with WHERE-clause literals in it. Allowing the schema and listing
exclusions makes that a denylist, so the next text-bearing view somebody adds is permitted by
default; naming relations keeps the default deny. `excluded_relations` exists for the one case
where a schema really is metadata apart from a known few, which is `pg_catalog`.

Two things to know before writing one:

- **Some dialects have no `information_schema` and address their catalog unqualified.** Oracle's
  dictionary views are public synonyms, so `FROM dba_tables` is idiomatic. List those without a
  schema; the gate permits an unqualified name only when the scope names it.
- **sqlglot must have your dialect, or `sql` cannot work at all.** It has none for DB2 or Vertica,
  so the gate refuses at dialect resolution — before any scope is read. `db2.py` therefore declares
  no scope and says why. The typed commands still work; only `sql` is affected.

A test asserts no declaration opens a schema where user tables live (`public`, `dbo`, `system`, …).
That is the exposure this design creates, and it is the same one `api_allowlist` has: pushing policy
to connectors means a careless declaration can widen it.

### If your source speaks SQL but is not SQLAlchemy-backed

Inherit `SqlCatalogPassthrough` (`agent/sql_passthrough.py`) and implement one method:

```python
class BigQueryMetadataProbe(SqlCatalogPassthrough):
    sql_dialect = "bigquery"          # the gate parses against this; required

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        iterator = self._client.query(query).result(max_results=limit)
        columns = [field.name for field in iterator.schema]
        return CatalogRows(columns=columns, rows=[...])

    def __exit__(self, *exc: object) -> None:   # yours: what closing means differs
        self._client.close()
```

Snowflake, BigQuery and the SQLAlchemy family each had their own `sql`, differing only
in how the driver yields columns and rows — a DictCursor, a `RowIterator` with a schema, a
`CursorResult`. `rows_from_mappings` handles the dict-per-row shape for you.

**The base owns the fetch-one-past-the-limit convention, and that is the reason it exists.**
`truncated` is computed by comparing rows returned against the limit, so an adapter that fetches
exactly `limit` reports `truncated: false` for a result set that was cut short — and an agent then
concludes it has seen every table in the catalog. Your `execute_catalog_query` is handed
`limit + 1` already; return everything asked for, do not re-clamp, and do not fetch the whole
result set to slice it afterwards, because on a paged API the discarded pages are real requests.

### If your source has a REST API

Inherit `RestApiPassthrough` (`agent/rest_passthrough.py`) rather than writing an `api` method.
The gate validates the _input_ either way — that comes from `scoped_path_param` — but the _call_ is
where every connector was getting something slightly different: a bare `requests.get` instead of
the connector's own session (which on Hex means escaping the rate limiter installed in
`HexApi.__init__`), a missing timeout, a missing `raise_for_status` so a 403 body reaches the agent
as though it were a listing, or the wrong base URL.

```python
class HexMetadataProbe(RestApiPassthrough):
    api_allowlist = ("GET /projects", "GET /projects/{id}/runs")

    def __init__(self, api: HexApi) -> None:
        self.api_session = api.session
        self.api_base_url = api.base_url

    def api_headers(self) -> Dict[str, str]:
        return api._auth_header()      # the connector's scheme, not a restated one
```

Override `api_fetch_json(url)` where the connector's own fetcher does more than `requests` does —
Mode's logs a curl equivalent and counts rate-limit retries, and a probe that bypassed it would
behave differently from ingestion on the same call.

**Writing the allowlist is the part nothing can do for you.** Two rules earned the hard way:

- **A relation being in a metadata API does not make it metadata.** Hex's `/cells` returns
  `SqlCell.sql_source` — the raw SQL of a notebook cell, so a `WHERE` literal is a row value
  arriving by another route. It is excluded for exactly the reason `sql_gate` excludes
  `pg_stat_statements`. `/projects/export` embeds the same SQL.
- **A `{placeholder}` matches any single segment, literal siblings included.** `GET /projects/{id}`
  also permits `GET /projects/export`. Where a sibling route exists that you do not want reachable,
  do not allowlist the `{id}` shape above it — Hex omits that entry for this reason, and loses
  nothing, because its typed commands already return project metadata.

Leaving an allowlist unset is not a way to allow everything: it permits nothing, and the refusal
says the _provider_ is incomplete rather than blaming the caller's path.

### If your source IS in the SQL family

`probe_filter_target` is not a framework hook and is deliberately absent from the table above:
nothing in `agent/` reads it. It is the SQL family's own extension point, consulted by the
`get_identifier` shim in `sql_probe.py` that `SQLCommonConfig.probe_match_target` routes to.
Override it when your real `Source` is not a `SQLAlchemySource`, so that shim has no
`get_identifier` to call — `RedshiftConfig` and `UnityCatalogSourceConfig` are the two that do:

```python
def probe_filter_target(
    self, schema: str, entity: str, warn: Callable[[str], None]
) -> Optional[str]:
    """The exact string ingestion filters table_pattern/view_pattern against,
    or None to let the shim keep resolving it."""
```

Call `warn` if you fall back to something less precise than your real ingestion identifier; it
feeds the same warnings list, deduplicated by message so one connector-wide reason is reported once.

### Checklist

- [ ] `probe_provider_class()` on the config, `for_config(config)` on the provider
- [ ] `@probe_method` on each listing, with `kind=` and `row_limit_param=` where they apply
- [ ] `sql_dialect` / `api_allowlist` present if any method declares a scoped parameter
- [ ] `Filters(...)` on any pattern field whose name does not follow the subtype
- [ ] verdicts checked against what ingestion computes for the same inputs
- [ ] `pytest tests/unit/agent/test_probe_contract.py` — the registry-wide scan covers you now
- [ ] listings return metadata only, and report degradation as a warning instead of an empty result

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
@probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
    """Run a read-only catalog query."""
    ...                     # `query` scope-checked, `limit` already clamped

@probe_method(name="api", scoped_path_param="path")
def api(self, path: str) -> object:
    """Fetch one listed read endpoint."""
    ...                     # `path` has already been allowlist-checked
```

There are three such declarations, and every one of them exists so the framework can act before
the method runs:

| Declaration         | What the framework does first                               |
| ------------------- | ----------------------------------------------------------- |
| `scoped_sql_param`  | scope-checks the query (`sql_gate`), refusing anything else |
| `scoped_path_param` | allowlist-checks the path (`api_gate`), GET only            |
| `row_limit_param`   | clamps the value into `1..MAX_PROBE_ITEMS`                  |

Enforcement lives in `probe_methods._enforce_gates` and `_bounded_kwargs`, never in the method — a
connector cannot forget a check it does not perform. Same split as `Filters(...)` on a config
field: declare the fact, let the framework act on it. A declaration naming a parameter that does
not exist raises at decoration time, because a typo would otherwise gate nothing silently.

**Declare `row_limit_param` on anything that takes a `limit`,** not just on `sql`. The getter
fetches `limit + 1` items, so an unclamped limit is a fetch the connector really performs and
trimming the output afterwards is too late — and `limit=-1` slices to `items[:-1]`, quietly
dropping the last item and then reporting the result as truncated.

**The declaration is the only thing the framework can see.** A method that takes a query and
declares nothing runs completely unchecked, and `probe methods` still advertises it. Nothing at
decoration time can tell that parameter apart from a harmless one, so
`tests/unit/agent/test_probe_contract.py` scans every registered connector for parameters that
look dangerous (`query`, `sql`, `path`, `url`, `limit`, …) and fails on any that no declaration
covers. It is a tripwire, not a boundary — renaming a parameter defeats it — which is why it lives
in a test where it is greppable and arguable rather than as a hard import-time failure.

An earlier design gave these their own CLI commands so that `probe run` had no path to a query
surface at all. That was a stronger _kind_ of guarantee — structural rather than enforced — but it
bought it with two parallel execution paths, and it left `probe methods` an incomplete picture of
what a connector could do. The guarantee is now "the channel is gated by a declaration the
framework enforces," which is worth knowing when reviewing a new `scoped_*` declaration.

A provider supplies what each gate needs: `sql_dialect` (a name sqlglot resolves) for queries, and
`api_allowlist` for paths. Either one missing refuses the call and says the provider is what is
incomplete — rather than guessing a grammar, or reporting an unlistable path as though the caller
had chosen a bad one. If your connector's dialect name differs from SQLAlchemy's (`postgresql` vs
`postgres`), add it to the map in `sqlalchemy_probe.py`.

### Turning the passthroughs off

`DATAHUB_PROBE_DISABLE_RAW_ACCESS=true` refuses every command that takes a caller-supplied query or
path, for an operator who does not want an agent issuing its own SQL against a source at all. Typed
listings keep working, so recipe diagnosis still functions. It is an environment variable rather
than a recipe field because the agent authors the recipe — a field there would let it grant itself
the access. Set it where the probe runs (the ingestion executor).

### The scope gate

`agent/sql_gate.py` parses with sqlglot and permits only a single `SELECT` whose every table
reference resolves into the dialect's catalog schemas. Two of its rules are not obvious:

- **A relation in a catalog schema is not automatically metadata.** `pg_stat_statements` and
  `pg_stat_activity` carry the literal text of user queries, values in `WHERE` clauses included, so
  they are excluded by name even though `pg_catalog` is permitted. Query history reaches the same
  place three ways per dialect and each is stopped by a different rule — worth knowing when adding
  a schema to the allowlist, because only the first of these travels with it:

  | Surface                                          | Refused by                                                                  |
  | ------------------------------------------------ | --------------------------------------------------------------------------- |
  | `pg_catalog.pg_stat_statements`                  | the query-text exclusion list, by name                                      |
  | `snowflake.account_usage.query_history`          | the schema allowlist (`ACCOUNT_USAGE` is not a permitted schema)            |
  | `information_schema.query_history()` (Snowflake) | the vendor-function rule — it is a table function inside a permitted schema |

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

### When the display name is not the address

Mode addresses objects by opaque token while the probe addresses them by the display name a
pattern is matched against, so each space-scoped command spends one spaces listing resolving the
name. Any BI source that shows a name while addressing by an internal ID lands here — Tableau's
LUIDs, Sigma, Qlik.

Resolve once per command and no more. The cost is inherent — each `probe run` is a fresh process
holding only what the caller typed — but it multiplies if one command fans out to sub-fetches that
each resolve independently: when a single command listed a Space's reports _and_ datasets, it
listed every space in the workspace twice. One command per listing is what fixed that, and
`test_a_space_scoped_command_lists_spaces_exactly_once` pins it. Do not add a convenience wrapper
that revives the fan-out.

### One hook names the provider, and it used to be two

`probe_provider_class()` is the whole answer: `probe methods` describes that class and `probe run`
builds it through the provider's own `for_config`. It was two hooks — a `build_probe_provider()` on
the config as well — and the pair could disagree. For Snowflake and BigQuery it did: both inherited
the SQLAlchemy answer for discovery while executing against their own client, so each advertised six
typed getters its provider does not have, every one of which failed at invocation with `no probe
method bound for command 'columns'` after the recipe had validated and a connection had opened.

Worth knowing because it is the argument against adding a second naming site back for convenience.
A test can catch two hooks disagreeing; one hook cannot disagree with itself.

**Probe output is metadata only** — names, types, constraints, DDL, counts. Never table rows,
column values, or message payloads. This one is convention, enforced by review and by the docstring
rule on `@probe_method`: nothing checks a return value's contents, and `agent/redact.py` is not the
net — it masks credentials drawn from the recipe, not data drawn from the source.

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
- **The contract scan is registry-wide, so a new connector is covered the moment it registers.**
  You do not add a case to `test_probe_contract.py`; you either declare the gate it asks for or
  argue in review why the parameter is safe. If you add a rule there, prove it fires — the file
  keeps a deliberately-bad provider per rule for exactly that reason.
- **Filter targets.** If the connector has a `get_identifier` equivalent, assert the probe's
  target equals what ingestion computes for the same inputs. `test_sql_filter_target.py` covers
  the SQLAlchemy family, including the Redshift schema override.
- **The degrade path.** A 404 on one sub-listing produces an empty result **and** a warning; auth
  failures and 5xx raise.
- **The connector's existing suites must pass unedited.** A probe adds to a connector; it does
  not change it.
