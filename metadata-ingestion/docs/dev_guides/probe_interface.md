# Adding a probe to a source

The probe interface lets someone — a person at a terminal, or an AI coding assistant — ask a
live source _"what is in you, and what would my recipe actually pick up?"_ before running an
ingestion. This guide explains how it works and how to add one to a connector.

## Why it exists

Writing a recipe is guesswork until you run it. You choose a source type, guess at the config
fields, guess at the `AllowDenyPattern`s, run an ingestion and read the report to find out what
you got. The probe answers those questions up front, and it answers them **the way ingestion
will** — which is the part that makes it useful rather than merely convenient.

Everything prints JSON and every failure has a distinct exit code, so a caller can tell "your
input was wrong" (2) from "I could not reach the source" (3).

## The two mechanisms

A connector can implement either or both. They are different contracts, not two spellings of
one thing.

### 1. Hierarchy walking — `probe shape`, `probe list`

The connector **declares its levels**; the framework walks them.

```
probe shape → Schema → Table → Column          (Redshift)
              Database → Schema → Table → Column   (Postgres, mssql, Snowflake)
              Topic                            (Kafka)
              Space → {Report → Query, Dataset}    (Mode — branching)
```

`probe list [--parent …]` lists the children at a level, one `--parent` per level descended.
Every object returned reports whether the recipe's filters would **include** it, and if not,
which pattern excluded it:

```json
{
  "name": "orders",
  "kind": "Table",
  "fqn": "public.orders",
  "pattern_field": "table_pattern",
  "included": false,
  "excluded_by": "table_pattern"
}
```

The output shape is uniform across every connector. That is what lets a caller write
"will this be ingested?" logic once.

### 2. Metadata getters — `probe methods`, `probe run`

`@probe_method` marks a method as an agent-callable command. Its parameters become CLI flags
and **its docstring becomes the help text**, so a caller discovers capability at runtime:

```shell
datahub recipe probe methods --recipe r.yml
datahub recipe probe run columns --recipe r.yml --schema public --table orders
```

Unlike the walk, the output shape is per-connector and self-describing.

**Probe output is metadata only** — names, types, constraints, DDL, counts. Never table rows,
column values, or message payloads.

## Adding hierarchy walking

### Declare the levels

```python
# mysource_probe.py
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes, DatasetSubTypes

def _schemas(client, config, parent_path):        # -> Sequence[str]
    return client.list_schemas()

def _tables(client, config, parent_path):
    return client.list_tables(parent_path[0])     # parent_path[0] is the schema

MYSOURCE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(DatasetContainerSubTypes.SCHEMA, list_names=_schemas),
        ProbeLevel(DatasetSubTypes.TABLE, list_names=_tables,
                   parent=DatasetContainerSubTypes.SCHEMA),
    ],
)
```

A lister receives `(client, config, parent_path)` and returns **names**. It does not filter and
it does not build nodes — the framework does both.

`parent=` defines the tree. Edges, not list order, determine the shape, so a level is
self-describing and reordering the declaration cannot silently change the hierarchy. Two levels
sharing a `parent` branch (see Mode: a Space holds both Reports and Datasets), in which case
`probe shape` reports `"linear": false` and an ambiguous name must be qualified as
`--parent 'Report:my_report'`.

### Wire the config hooks

A config that owns exactly one `ClientProbe` gets `probe_hierarchy`, `probe_shape`, and
`list_probe_children` for free from `ProbeableConfigMixin` — inherit it and override the one
method it asks for:

```python
class MySourceConfig(ProbeableConfigMixin, ...):
    @classmethod
    def _client_probe(cls) -> ClientProbe:
        from datahub.ingestion.source.mysource_probe import MYSOURCE_PROBE
        return MYSOURCE_PROBE
```

The lazy import inside `_client_probe()` keeps the probe module off the config's import path.
The mixin's `probe_hierarchy` calls `_client_probe().hierarchy()`, so it inherits the
"must not connect" guarantee for free; a **branching** probe needs no special case either —
`ClientProbe.hierarchy()` already raises `ProbeBranchesError` for a tree that has no single
chain, and the mixin's `probe_shape()` (also derived from `_client_probe()`) is already the
right accessor for it. Mode's config overrides only `_client_probe()`, exactly like a linear
connector; the mixin does the rest.

**Variants are selected by class, never by a source-type list.** `TwoTierSQLAlchemyConfig`
overrides `_client_probe()` to point at the two-tier probe, so MySQL/Hive/ClickHouse/Teradata get
`Database → Table → Column` by inheriting it. Postgres and mssql do the same to add their
`Database` level on top of the generic Schema-top probe they'd otherwise inherit.

If a config doesn't fit that shape — it delegates to another source's own connection object
(`bigquery-queries`/`snowflake-queries` reuse BigQuery/Snowflake's connection config) rather than
owning a `ClientProbe` of its own — implement `probe_hierarchy`/`list_probe_children` directly
instead of using the mixin, exactly as those two configs do.

### Filtering

A level resolves its `AllowDenyPattern` from the config by convention: `Table` →
`table_pattern` or `table_patterns`. Where the field name doesn't follow from the level's
subtype — because config naming follows the source's own vocabulary, as it should — declare it
on the field:

```python
collection_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(...)
```

The connector already knew this fact; declaring it means the probe reads it instead of guessing
from the name.

## Adding metadata getters

```python
class MySourceConfig(...):
    def build_probe_provider(self) -> ProbeProvider:
        return MyMetadataProbe(self.get_client())
```

The provider is any context manager whose methods carry `@probe_method`. Discovery uses
`dir()`, so **annotating the connector's own methods works** — a provider can be the source
itself:

```python
class MySource(...):
    @probe_method(name="data_sources")
    def _get_data_sources(self) -> Dict[int, dict]:
        """Warehouse connections this workspace can query. Returns the raw API records."""
```

If the provider needs connector methods but must not run `__init__` (which typically opens a
connection and emits telemetry), build an uninitialized instance with `__new__` and prime only
the attributes those methods touch. `ModeSource.for_probe()` is the worked example.

## The rules that matter

These are the ones learned the expensive way. Each has cost a review round.

**Reuse the connector's fetch; never its policy.** Ingestion degrades on error to salvage
partial metadata; a diagnostic must not, because distinguishing _"nothing here"_ from _"I could
not look"_ is its entire job. When a connector method looks like the right thing to delegate to
but filters internally or swallows errors, reach for the layer beneath it — not a
reimplementation.

**Beware the function whose name matches your intent.** `_get_definitions_map()` sounds like a
definitions fetcher; it is a lossy `{name: source}` cache for template expansion.
`_get_space_name_and_tokens()` sounds like a space lister; it applies `space_pattern` itself, so
delegating to it would make a _denied_ space vanish instead of appearing as an excluded node.
Read the target before delegating.

**Filter on the same string ingestion does.** `AllowDenyPattern` uses start-anchored
`re.match`, so `deny: ["^orders$"]` does not match `public.orders`. If ingestion filters a
qualified name, the probe must too — use `filter_target` and point it at the connector's own
identifier function rather than re-deriving it. Druid is the instructive case: its ingestion
matches the _bare_ table name, so any structural "qualify it with its parents" rule gets Druid
wrong, and reuse gets it right with no special case.

**Never construct a `Source`.** `__init__` typically opens connections and emits telemetry. Use
a client, or an uninitialized instance.

**Report degradation, don't hide it.** A 404/403 on one sub-listing should degrade that level to
empty and say so; auth failures and 5xx should raise. `soft_on_status(403, 404, context=…)`
gives you the split. A classifier that degrades reports via `ctx.warn(…)`, which lands on
`ProbeResult.warnings`, deduped. An empty result **with** warnings means "part of this could not
be read" — never "this is empty".

**The hierarchy is a claim about the source.** Shallower than reality and a caller cannot reach
real objects; deeper and it is offered navigation the recipe cannot use. Add a level only where
the source can enumerate the tier **and** ingestion can address more than one of them. Trino's
`database` is a single config-supplied catalog, so it gets no Catalog level; Postgres iterates
`pg_database` filtered by `database_pattern`, so it does.

**A probe method returning a raw dict returns a raw dict.** The framework redacts what it can
identify by field type; anything else is the annotating developer's call. If you want precision,
return typed objects rather than dicts.

## Reference

`ProbeLevel` fields: `kind`, `parent`, plus exactly one lister —

| field           | use                                                                                |
| --------------- | ---------------------------------------------------------------------------------- |
| `list_names`    | one lister, level-wide kind and pattern                                            |
| `sources`       | several listers, each with its own kind and pattern (tables + views)               |
| `list_items`    | one lister yielding `(name, kind, pattern_field)` per item                         |
| `pattern_field` | override the conventional field; `UNFILTERED` if the source offers no filter       |
| `filter_target` | the exact string the pattern is matched against                                    |
| `classify`      | full verdict override, for structural exclusions the user's patterns don't express |

`Verdict(included: bool, excluded_by: Optional[str])` — `Verdict.include()` for the common case.
`excluded_by` names a `*_pattern` field or a structural reason (`"default_schema"`,
`"system_object"`, `"unnamed"`).

## Testing expectations

- A fake client exercising each level, including a **denied** object asserted as
  `included: false` with the right `excluded_by`.
- The degrade path: a 404 on one sub-listing produces an empty level **and** a warning; auth
  failures and 5xx raise.
- If the connector has a `get_identifier`-equivalent, assert the probe's filter target equals
  what ingestion computes for the same inputs.
- The connector's **existing** suites must pass unedited. A probe adds to a connector; it does
  not change it.
