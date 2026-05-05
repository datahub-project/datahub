# bump_schema_versions.py

A pre-commit CLI tool that automatically bumps `schemaVersion` annotations on PDL aspect files whenever their content — or the content of any record they include — changes relative to the base branch.

## Why this exists

DataHub's metadata model is defined in PDL files under `metadata-models/src/main/pegasus/`. Each **aspect** file carries a `schemaVersion` field inside its `@Aspect` annotation. This version must be incremented whenever the aspect's schema changes so that downstream consumers (schema registries, compatibility checks, migration tooling) can detect and react to the change. The `schemaVersion` is optional and when
missing, defaults to 1.

Without automation this is easy to forget, especially for **implicit** changes — where a shared base record (e.g. `TimeseriesAspectBase`) is modified and every aspect that includes it is silently affected.

## Usage

Run from the repository root:

```
python3 scripts/bump_schema_versions.py [OPTIONS]
```

### Options

| Flag                   | Default       | Description                                                                                                       |
| ---------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------- |
| `--base-branch BRANCH` | auto-detected | Branch to compare against. Auto-detected from `origin/HEAD`; falls back to `$MAIN_BRANCH` env var, then `master`. |
| `--dry-run`            | off           | Preview changes without writing any files.                                                                        |
| `--verbose` / `-v`     | off           | Show per-file reasoning (why each file was skipped or bumped).                                                    |
| `-h` / `--help`        | —             | Show help and exit.                                                                                               |

### Environment variables

| Variable      | Default                            | Description                                                                                       |
| ------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------- |
| `PDL_ROOTS`   | `metadata-models/src/main/pegasus` | Colon-separated list of PDL source roots to scan. Useful when PDL files live in multiple modules. |
| `MAIN_BRANCH` | `master`                           | Base branch fallback when `origin/HEAD` is not configured locally.                                |

**Example — multiple PDL roots:**

```bash
PDL_ROOTS="metadata-models/src/main/pegasus:other-module/src/main/pegasus" \
  python3 scripts/bump_schema_versions.py
```

**Pre-commit hook with custom roots:**

```yaml
- repo: local
  hooks:
    - id: bump-schema-versions
      name: Bump PDL schema versions
      entry: python3 scripts/bump_schema_versions.py
      language: python
      types: [file]
      pass_filenames: false
      env:
        PDL_ROOTS: "metadata-models/src/main/pegasus:other-module/src/main/pegasus"
```

### Examples

```bash
# Normal run — bumps all affected aspects in place
python3 scripts/bump_schema_versions.py

# Preview what would be bumped without touching any files
python3 scripts/bump_schema_versions.py --dry-run

# Verbose preview — shows skipped files and reasons
python3 scripts/bump_schema_versions.py --dry-run --verbose

# Compare against a non-default base branch
python3 scripts/bump_schema_versions.py --base-branch master

# Short verbose flag
python3 scripts/bump_schema_versions.py -v
```

## How it works

### Step 1 — Find changed PDL files

```
git diff --name-only --diff-filter=ACM <base-branch> -- *.pdl
```

This captures added, copied, and modified PDL files relative to the base branch, including uncommitted staged and unstaged changes. **Untracked files** (not yet `git add`ed) are not detected.

### Step 2 — Build the include graph

All PDL files under `metadata-models/src/main/pegasus/` are scanned once to build a **reverse include map**: for every record, which files include it?

PDL `includes` resolution follows the language rules:

```pdl
namespace com.linkedin.dataset
import com.linkedin.timeseries.TimeseriesAspectBase

record DatasetUsageStatistics includes TimeseriesAspectBase { ... }
```

`TimeseriesAspectBase` is resolved to `com.linkedin.timeseries.TimeseriesAspectBase` via the `import` statement (namespace is used as a fallback for unimported names).

### Step 3 — Transitive propagation (BFS)

Starting from the directly changed files, the tool does a breadth-first search through the reverse include graph to find every file that (directly or transitively) depends on any changed record. From that reachable set, only files with an `@Aspect` annotation are eligible for a version bump.

```
TimeseriesAspectBase.pdl  ← changed
  └─ included by DatasetUsageStatistics.pdl  (aspect → bumped)
  └─ included by DatasetProfile.pdl          (aspect → bumped)
  └─ included by ChartUsageStatistics.pdl    (aspect → bumped)
  └─ ...
```

### Step 4 — Bump the version

For each affected aspect:

| Situation                                     | Action                                   |
| --------------------------------------------- | ---------------------------------------- |
| File is new (not on base branch)              | Already defaults to `1`; no write needed |
| `schemaVersion` absent on base branch         | Treat base version as `1`; bump to `2`   |
| `schemaVersion` present on base branch        | Bump to `base_version + 1`               |
| Current file already has the expected version | Skip (idempotent)                        |

The annotation is updated in place, preserving existing formatting:

**Before:**

```pdl
@Aspect = {
  "name": "globalTags"
}
```

**After (first change):**

```pdl
@Aspect = {
  "name": "globalTags",
  "schemaVersion": 2
}
```

**After (trailing-comma style, e.g. timeseries aspects):**

```pdl
@Aspect = {
  "name": "datasetUsageStatistics",
  "type": "timeseries",
  "schemaVersion": 2
}
```

## Output

Each processed file prints one line:

```
BUMP  <path>  v<old> → v<new>  [<reason>]
SKIP  <path>  (reason)          ← only with --verbose
```

Reason labels:

- `[direct change]` — the aspect file itself was modified
- `[implicit (includes changed record)]` — a record this aspect includes was modified

### Example output

```
Comparing against branch: master
Found 1 directly changed PDL file(s):
  metadata-models/src/main/pegasus/com/linkedin/timeseries/TimeseriesAspectBase.pdl

BUMP  metadata-models/.../chart/ChartUsageStatistics.pdl         v1 → v2  [implicit (includes changed record)]
BUMP  metadata-models/.../common/Operation.pdl                    v1 → v2  [implicit (includes changed record)]
BUMP  metadata-models/.../dashboard/DashboardUsageStatistics.pdl  v1 → v2  [implicit (includes changed record)]
BUMP  metadata-models/.../dataset/DatasetProfile.pdl              v1 → v2  [implicit (includes changed record)]
BUMP  metadata-models/.../dataset/DatasetUsageStatistics.pdl      v1 → v2  [implicit (includes changed record)]
...

Summary: 9 bumped, 0 skipped, 0 errors
```

## Exit codes

| Code | Meaning                                  |
| ---- | ---------------------------------------- |
| `0`  | Success (including "nothing to bump")    |
| `1`  | One or more files could not be processed |

## Pre-commit integration (planned)

Add to `.pre-commit-config.yaml`:

```yaml
- repo: local
  hooks:
    - id: bump-schema-versions
      name: Bump PDL schema versions
      entry: python3 scripts/bump_schema_versions.py
      language: python
      types: [file]
      pass_filenames: false
```

## Scope and limitations

- Only PDL files under PDL_ROOTS (or default `metadata-models/src/main/pegasus/`) are scanned for the include graph.
- Only records with an `@Aspect` annotation get a version bump; plain records and union types are skipped.
- The tool does not validate that the schema change is backwards-compatible — it only tracks that _a_ change occurred.
- Deleted aspects (files removed in the diff) are not processed.
