# DataHub Lineage CLI - Agent Context

Best practices for AI agents consuming `datahub lineage`.

## Direction

- Use `--direction upstream` to find what feeds into an entity (data sources)
- Use `--direction downstream` to find what depends on an entity (impact analysis)
- Upstream is the default; always specify direction explicitly for clarity

## Hops

- Default is 3 (full transitive lineage, 3+ means unlimited)
- Use `--hops 1` for immediate neighbors only
- Use `--hops 2` for two levels of dependencies
- The `--count` default (100) limits result size even with full hops

## Output

- Use `--format json` for machine consumption
- Use `--format table` for human-readable display (default)
- JSON output includes urn, type, hops, direction, platform, and name per entity

```bash
# Upstream sources for a dataset
datahub lineage --urn "urn:li:dataset:(...)" --direction upstream --format json

# Full downstream impact (all hops)
datahub lineage --urn "urn:li:dataset:(...)" --direction downstream --hops 3

# Column-level lineage (datasets only)
datahub lineage --urn "urn:li:dataset:(...)" --column customer_id --direction upstream
```

## Column-Level Lineage

- Use `--column <field_name>` to trace column-level lineage
- Only works with dataset URNs (not charts, dashboards, etc.)
- Results show which columns in upstream/downstream datasets are connected

## Path Between Entities

Use `datahub lineage path` to find the specific path between two entities:

```bash
datahub lineage path --from "urn:li:dataset:(...)" --to "urn:li:dashboard:(...)"
```

## Common Patterns

```bash
# Impact analysis: what breaks if I change this table?
datahub lineage --urn "<URN>" --direction downstream --hops 3 --format json

# Root cause: where does this data come from?
datahub lineage --urn "<URN>" --direction upstream --hops 3 --format json

# Count downstream dependents
datahub lineage --urn "<URN>" --direction downstream --hops 3 --format json | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"
```

## Error Handling

- If lineage returns 0 results, the entity may not have lineage ingested — report this honestly
- If `--column` is used with a non-dataset URN, the command will error
