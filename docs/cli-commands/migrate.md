# datahub migrate

The `migrate` group of commands allows you to perform certain kinds of migrations.

All migration commands share the same engine, so they behave consistently:

- **All aspects are migrated.** The set of aspects carried to the new URN is sourced from the
  entity registry, so every user-authored aspect (ownership, tags, glossary terms, documentation,
  structured properties, and so on) is transferred — not a fixed subset. Timeseries aspects (usage
  statistics, profiles) and system-managed aspects (browse paths, incidents summary) are not
  migrated.
- **All incoming references are repointed.** Every entity that references a migrated URN via a
  modeled relationship — lineage, dashboard/chart references, **assertions**, etc. — is updated to
  point at the new URN, table- and column-level.

## dataplatform2instance

The `dataplatform2instance` migration command allows you to migrate your entities from an instance-agnostic platform identifier to an instance-specific platform identifier. If you have ingested metadata in the past for this platform and would like to transfer any important metadata over to the new instance-specific entities, then you should use this command. For example, if your users have added documentation or added tags or terms to your datasets, then you should run this command to transfer this metadata over to the new entities. For further context, read the Platform Instance Guide [here](../platform-instances.md).

This command migrates the following entity types: datasets, charts, dashboards, dataflows, datajobs, and containers — together with all of their (registry-defined) aspects.

Options:

- `--platform` (required): The data platform to migrate (e.g. `hive`, `snowflake`, `powerbi`).
- `--instance` (required): The target platform instance name.
- `--dry-run` / `-n`: Preview what will be migrated without making changes.
- `--force` / `-F`: Skip the confirmation prompt.
- `--keep`: Preserve old entities instead of soft-deleting them.
- `--hard`: Hard-delete old entities instead of soft-delete.
- `--env`: The environment/fabric to filter on (default: `PROD`).
- `--on-conflict`: How to handle entities that already exist at the target. One of `overwrite` (default), `patch`, `prompt`, or `preserve` (see [Conflict strategies](#conflict-strategies)).
- `--skip-on-error`: Continue migrating remaining entities when one fails, instead of aborting.
- `--entity-types`: Comma-separated list of entity types to migrate (default: all). Available: `dataset`, `chart`, `dashboard`, `dataFlow`, `dataJob`.
- `--checkpoint-file`: Path to a checkpoint file for resumable migrations. Already-migrated source URNs are read from this file and skipped; newly migrated URNs are appended on success. The file is created automatically on first write. Not written during `--dry-run`; errored pairs (under `--skip-on-error`) are not checkpointed, so they retry on resume.

**_Note_**: Timeseries aspects such as Usage Statistics and Dataset Profiles are not migrated over to the new entity instances, you will get new data points created when you re-run ingestion using the `usage` or sources with profiling turned on.

### Dry Run

```console
datahub migrate dataplatform2instance --platform powerbi --instance my_instance --dry-run --skip-on-error
Starting migration: platform:powerbi, instance=my_instance, force=False, dry-run=True
This command will migrate DATASET, CHART, DASHBOARD, DATAFLOW, DATAJOB and CONTAINERS.
No dataset entities found without instance, skipping.
No chart entities found without instance, skipping.
No dashboard entities found without instance, skipping.
No dataFlow entities found without instance, skipping.
No dataJob entities found without instance, skipping.
100% (398 of 398) |##############| Elapsed Time: 0:00:00
[Dry Run] Migration Report:
--------------
[Dry Run] Migration Run Id: container-migrate-f324880f-...
[Dry Run] Num entities created = 0
[Dry Run] Num entities affected = 0
[Dry Run] Num entities migrated = 0
```

### Real Migration (with soft-delete)

```console
datahub migrate dataplatform2instance --platform hive --instance warehouse
Starting migration: platform:hive, instance=warehouse, force=False, dry-run=False
This command will migrate DATASET, CHART, DASHBOARD, DATAFLOW, DATAJOB and CONTAINERS.
Will migrate 4 urns such as [...]
New urns will look like ['urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.SampleHiveDataset,PROD)', ...]
Ok to proceed? [y/N]: y
...
Migration Report:
--------------
Migration Run Id: migrate-f5ae7201-...-dataset
Num entities created = 4
Num entities affected = 0
Num entities migrated = 4
```

## instance2instance

The `instance2instance` migration command allows you to migrate entities from one platform instance to another. This is useful when you need to rename or consolidate platform instances, for example when merging two DataHub environments or renaming an instance after an infrastructure change.

Like `dataplatform2instance`, this command migrates datasets, charts, dashboards, dataflows, datajobs, and containers. It rewrites URNs by replacing the old instance prefix with the new one and updates all incoming relationships (lineage, dashboard references, etc.) to point to the new entities.

When the target entity already exists, the command uses a strategy controlled by `--on-conflict`
(see [Conflict strategies](#conflict-strategies) below).

**Merge limitation**: Full aspect-level merge (via the Patch API) is only supported for **dataset** entities. For charts, dashboards, dataflows, and datajobs, the merge path falls back to overwrite when the target entity already exists.

**Container limitation**: Containers are migrated via a separate legacy code path that always overwrites the target. `--on-conflict`, `--skip-on-error`, and `--checkpoint-file` apply only to the entity migration (datasets, charts, dashboards, dataflows, datajobs) — not to containers. In practice this is rarely an issue: containers are a small bounded set per migration and are typically re-created by ingestion.

### Conflict strategies

`--on-conflict` controls what happens when the target URN already exists. It is available on all
`migrate` commands.

- `patch`: Additively merge ownership, tags, terms, and lineage. Skip scalar fields (description, custom properties) that conflict.
- `overwrite`: Replace conflicting target aspects with source values.
- `prompt`: Ask interactively for each conflict.
- `preserve`: Leave the existing target **completely untouched** (no additive merge, no overwrite). Incoming references are still repointed to the existing target and the source is still deleted (unless `--keep`) — i.e. the existing target is _adopted_ in place of the source. Use this when the target is authoritative and you only want to redirect references and retire the source.

Defaults differ by command: `overwrite` for `dataplatform2instance`, `patch` for `instance2instance`, `overwrite` for `urns-mapping`.

Regardless of the chosen strategy, the following aspects are **never overwritten** during a merge (i.e. when the target already exists):

- **`status`**: The target's soft-delete state is authoritative. A soft-deleted source must not remove a live target.
- **`container`**: The target's parent container is preserved. For `urns-mapping`, containers are not migrated, so the source's container reference may point at an entity that is about to be deleted. For `dataplatform2instance` / `instance2instance`, the separate container migration step repoints container references via incoming-relationship rewriting.

These aspects are still cloned normally when the target does not exist (the clone path).

Options:

- `--platform` (required): The data platform (e.g. `snowflake`, `powerbi`).
- `--old-instance` (required): The source platform instance name.
- `--new-instance` (required): The target platform instance name.
- `--dry-run` / `-n`: Preview what will be migrated without making changes.
- `--force` / `-F`: Skip the confirmation prompt.
- `--keep`: Preserve old entities instead of soft-deleting them.
- `--hard`: Hard-delete old entities instead of soft-delete.
- `--env`: The environment/fabric to filter on (default: `PROD`).
- `--on-conflict`: Conflict resolution strategy: `patch` (default), `overwrite`, `prompt`, or `preserve` (see [Conflict strategies](#conflict-strategies)).
- `--skip-on-error`: Continue migrating remaining entities when one fails, instead of aborting.
- `--entity-types`: Comma-separated list of entity types to migrate (default: all). Available: `dataset`, `chart`, `dashboard`, `dataFlow`, `dataJob`.
- `--checkpoint-file`: Path to a checkpoint file for resumable migrations (see `dataplatform2instance` above).

**⚠️ Note**: `dataFlow` and `dataJob` should always be migrated together. DataJob URNs embed their parent DataFlow URN — migrating one without the other creates orphaned references. The CLI will prompt for confirmation if you attempt to separate them.

### When to Use Which Command

| Scenario                                                   | Command                                            |
| ---------------------------------------------------------- | -------------------------------------------------- |
| First time adding a platform instance to existing entities | `dataplatform2instance`                            |
| Renaming or consolidating platform instances               | `instance2instance`                                |
| Partial migration (specific entity types only)             | Either command with `--entity-types=dataset,chart` |

The key difference: `dataplatform2instance` migrates entities that have **no** platform instance to a new instance (prepending the instance prefix). `instance2instance` migrates entities from one **existing** instance to another (replacing the instance prefix). The default `--on-conflict` also differs: `overwrite` for p2i, `patch` for i2i.

### Dry Run

```console
datahub migrate instance2instance \
  --platform powerbi \
  --old-instance old_inst \
  --new-instance new_inst \
  --dry-run \
  --skip-on-error
Starting migration: platform:powerbi, old-instance=old_inst, new-instance=new_inst, force=False, dry-run=True, on-conflict=patch
This command will migrate DATASET, CHART, DASHBOARD, DATAFLOW, DATAJOB and CONTAINERS.
Found 689 dataset entities to migrate.
100% (689 of 689) |##############| Elapsed Time: 0:04:55
[Dry Run] Migration Report:
--------------
[Dry Run] Migration Run Id: migrate-i2i-cba165cc-...-dataset
[Dry Run] Num entities created = 689
[Dry Run] Num entities affected = 181
[Dry Run] Num entities migrated = 688
[Dry Run] Entities errored = 1
[Dry Run]   urn:li:dataset:(...,%_of_sales,PROD): URLDecoder: Illegal hex characters in escape (%) pattern
No chart entities found, skipping.
No dashboard entities found, skipping.
No dataFlow entities found, skipping.
No dataJob entities found, skipping.
```

### Real Migration with Merge

```console
datahub migrate instance2instance \
  --platform powerbi \
  --old-instance old_inst \
  --new-instance new_inst \
  --on-conflict patch \
  --skip-on-error
```

### Safe Exploration Workflow

When migrating between instances, a safe approach is:

1. **Dry run** to see what will happen:
   ```console
   datahub migrate instance2instance --platform powerbi --old-instance old --new-instance new --dry-run
   ```
2. **Migrate with --keep** to preserve the old entities as a safety net:
   ```console
   datahub migrate instance2instance --platform powerbi --old-instance old --new-instance new --keep
   ```
3. **Verify** the new entities look correct in the DataHub UI.
4. **Clean up** old entities manually with `datahub delete` if everything looks good.

## urns-mapping

The `urns-mapping` command migrates entities using an **explicit source → target URN mapping** that
you supply, rather than deriving targets from a platform-instance strategy. Use it for arbitrary URN
moves that `dataplatform2instance` / `instance2instance` can't express — for example a bulk rename
computed by an external process. It runs the same engine (clone all aspects, rewrite self- and
incoming references, delete the source), so behavior matches the other commands.

Options:

- `--mapping-file` (required): Path to a JSON file of source → target pairs. Either a list of
  objects, `[{"source": "urn:...", "target": "urn:..."}, ...]`, or a flat object,
  `{"urn:src": "urn:tgt", ...}`.
- `--dry-run` / `-n`, `--force` / `-F`, `--keep`, `--hard`, `--skip-on-error`, `--checkpoint-file`: as for the other commands.
- `--on-conflict`: `overwrite` (default), `patch`, `prompt`, or `preserve` (see [Conflict strategies](#conflict-strategies)).

Each pair's source and target must be the **same entity type**. Unlike the instance-migration
commands, `urns-mapping` does **not** stamp a `dataPlatformInstance` (a platform instance cannot be
recovered from a target URN), does not migrate containers (see note below), and does not enforce
dataFlow/dataJob parent-URN consistency — you own the exact mapping.

**Containers are not migrated.** Container URNs are GUID-based (derived from a container key), so
they cannot be included in a simple source → target mapping. The next ingestion run against the new
names will re-create the containers automatically, but any user-authored governance metadata on the
old containers (tags, terms, documentation, ownership) will be lost. `dataplatform2instance` and
`instance2instance` handle container migration via a dedicated code path; adding container support
to `urns-mapping` is a potential future enhancement.

**Ordering across cross-referenced entities**: when a single mapping migrates entities that
reference one another (e.g. a dashboard that consumes a dataset you are also migrating), list the
**referenced** entity before the **referrer**. Incoming references are repointed using the
relationship index, which only reflects references that existed before the run — so migrating the
referenced entity first ensures the referrer is repointed and then carries the updated reference
into its own migrated copy.

**Large or complex migrations**: for big migrations, or reference graphs where clean
referenced-before-referrer ordering isn't possible (bidirectional references, cycles), migrate in
stages — e.g. one entity type at a time — with `--keep`, verifying (in the UI or via the graph API)
that references have settled before running the next stage. Because a later stage's
incoming-reference rewrite reads the relationship index, letting it catch up between stages lets that
stage see the entities migrated earlier. Note this only helps references _across_ stages; references
between two entities migrated in the **same** stage still rely on the ordering above, so keep
mutually-referencing entities dependency-ordered within a stage.

**Nested URNs — user responsibility**: some entity URNs embed another entity's URN as part of their
identity. The migration engine does **not** automatically derive child mappings from parent mappings:

- **dataFlow / dataJob**: a `dataJob` URN embeds its parent `dataFlow` URN
  (`urn:li:dataJob:(urn:li:dataFlow:(...), jobId)`). When migrating a dataFlow, you **must** also
  include mappings for its child dataJob URNs; otherwise the dataJob key aspects become inconsistent
  with the entity URN. Automatic derivation of child URN mappings may be added in a future release.
- **schemaField**: column-level metadata stored on the dataset's `editableSchemaMetadata` aspect
  (tags, terms, descriptions applied via the UI) **is** migrated along with the dataset. `schemaField`
  entities — which carry their own `globalTags`, `glossaryTerms`, `documentation`, and structured
  property aspects — are **not** repointed by `urns-mapping`; they are typically re-created by the next
  ingestion run against the new dataset name. When only the **column casing** changes (the dataset URN
  stays the same), use [`schema-field-case`](#schema-field-case) instead, which re-anchors those
  `schemaField` aspects and `editableSchemaMetadata` entries onto the new field paths.

Example `mapping.json` (list form):

```json
[
  {
    "source": "urn:li:dataset:(urn:li:dataPlatform:snowflake,old_db.schema.events,PROD)",
    "target": "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_db.schema.events,PROD)"
  },
  {
    "source": "urn:li:dashboard:(looker,old.sales)",
    "target": "urn:li:dashboard:(looker,new.sales)"
  }
]
```

The equivalent flat-object form maps each source URN directly to its target:

```json
{
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,old_db.schema.events,PROD)": "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_db.schema.events,PROD)"
}
```

```console
datahub migrate urns-mapping --mapping-file ./mapping.json --dry-run
```

## schema-field-case

The `schema-field-case` command re-anchors column-level metadata after a connector changes the
**casing** of column names — for example after enabling Snowflake's
[`preserve_column_case`](https://docs.datahub.com/docs/generated/ingestion/sources/snowflake), or on
any connector that historically lowercased identifiers (e.g. Oracle). The dataset URN is unchanged;
only the field paths move (`product2id` → `Product2Id`), which strands the UI/API-authored metadata
that was keyed on the old paths. Matching is purely case-insensitive, so it works in **either
direction** — enabling case preservation (`product2id` → `Product2Id`) or the reverse, turning
lowercasing on for a source whose fields were mixed-case (`Product2Id` → `product2id`).

> **Not for dataset-URN casing changes.** Toggling `convert_urns_to_lowercase` re-cases the **dataset
> URN itself** (and the dataset portion of every `schemaField` URN), not the field paths — the whole
> dataset moves to a new URN. That is a dataset-level remap; use
> [`urns-mapping`](#urns-mapping) for it, not this command.

Unlike the other `migrate` commands, this does **not** rewrite entity URNs or use the migration
engine. It reconciles, per dataset, against the freshly re-ingested `schemaMetadata` (the source of
truth for the new casing):

- **`editableSchemaMetadata`** entries (descriptions, tags, terms edited in the UI) are rewritten in
  place onto the matching current field path.
- **`schemaField` entity** aspects (`globalTags`, `glossaryTerms`, `documentation`,
  `structuredProperties`, `businessAttributes`, and the other user-authored column aspects) are
  copied onto the correctly-cased `schemaField` URN. The old entity is soft-deleted by default (use
  `--keep-source-fields` to keep it).

> **Not re-anchored:** entities that _reference_ a column rather than living on it — native column
> assertions and incidents — are not repointed and will dangle on the soft-deleted source. Use
> `--keep-source-fields` if a dataset has those. Connector-emitted references (lineage,
> `fineGrainedLineage`, foreign keys) don't need repointing: the prerequisite re-ingestion re-emits
> them onto the new field path.

**Run order:** re-ingest the source with the new casing **first**, then run this command.

Matching is case-insensitive and handles both v1 and v2 field paths. It is conservative by default:

- **Ambiguous case-only collisions are never guessed.** If a single stranded field maps to two
  current fields that differ only by case (e.g. a historically lowercased `col` now split into `Col`
  and `COL`), it is reported for manual review and left untouched.
- **Existing destination metadata is preserved.** Tags, glossary terms, and structured properties are
  merged with whatever already sits on the correctly-cased field: tags/terms are unioned by URN
  (preserving source/immutable attribution), and structured properties are unioned by property URN.
  The same structured property carrying different values on each side, or any other aspect
  (`documentation`, `deprecation`, `businessAttributes`, …) that already differs on the destination,
  is reported rather than overwritten, and the source is kept so nothing is lost.
- **Re-running is safe** (idempotent): matched entries converge and merged aspects do not duplicate.

Use `--interactive` to resolve those clashes at the prompt instead of skipping them — you choose the
target for an ambiguous collision, and decide per-aspect whether to overwrite the destination on a
conflict. Without it, clashes are reported under "needing manual review" and left for you to handle.

Options:

- `--platform`: Platform to discover datasets for (e.g. `snowflake`, `oracle`). Ignored when
  `--urn` / `--urn-file` is given.
- `--platform-instance`: Platform instance to filter discovery on, if any. Works with or without a
  platform instance — everything is derived from the actual dataset URN.
- `--env`: Env (e.g. `PROD`) to filter discovery on, if any.
- `--urn` / `--urn-file`: Explicit dataset URN(s) to reconcile instead of discovery (repeatable /
  one URN per line).
- `--keep-source-fields` / `--delete-source-fields`: Keep or soft-delete the old `schemaField` entity
  after copying its aspects (default: delete). `editableSchemaMetadata` entries are always rewritten
  in place.
- `--include-soft-deleted` / `--exclude-soft-deleted`: Include soft-deleted `schemaField` entities
  when discovering stranded fields (default: exclude).
- `--interactive` / `--no-interactive`: Prompt to resolve each clash (ambiguous collision target,
  aspect-conflict overwrite) instead of skipping it (default: no). Ignored under `--dry-run`.
- `--dry-run` / `-n`: Report what would change without writing.
- `--force` / `-F`: Skip the confirmation prompt.

```console
# Preview against a single dataset
datahub migrate schema-field-case \
  --urn 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)' \
  --dry-run

# Reconcile every Snowflake dataset after enabling preserve_column_case
datahub migrate schema-field-case --platform snowflake --force
```

## Known limitations

These apply to all `migrate` commands.

- **Non-relationship references are not repointed.** URN values stored in non-relationship fields
  (e.g. a URN _value_ of a structured property on another entity) are not graph-indexed and
  therefore not discovered or rewritten by the migration engine.

- **Optimistic cross-pair rewriting under `--skip-on-error`.** The batch URN rewriter is built from
  _all_ pairs before the migration loop starts. If pair A fails and a later pair B's aspects
  reference A's source URN, that reference is still rewritten to A's (never-created) target URN.
  This is intentional: when the user retries failed pairs via `--checkpoint-file`, A's target is
  created and B's reference becomes correct. Removing failed pairs from the map would leave B
  pointing at A's source (which may be deleted), making retry harder.

- **`--on-conflict patch` and multi-downstream fine-grained lineage.** The Patch API keys
  fine-grained lineage entries on `(transformOperation, downstream, query)` and requires exactly one
  downstream per entry. FGL entries with 0 or 2+ downstreams will raise an error during merge. This
  is a pre-existing constraint of the Patch API, not specific to migration. In practice, field-level
  lineage is almost always N upstreams → 1 downstream, so this is unlikely to trigger. Use
  `--on-conflict overwrite` instead of `patch` if your data contains multi-downstream FGL entries.
