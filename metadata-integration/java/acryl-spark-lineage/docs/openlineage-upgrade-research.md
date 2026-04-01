# OpenLineage Upgrade Research: 1.38.0 → 1.45.0

**Date:** 2026-04-01
**Current version:** 1.38.0
**Target version:** 1.45.0 (released 2026-03-11)
**Gap:** 7 minor versions (1.39.0, 1.40.0, 1.41.0, 1.42.1, 1.43.0, 1.44.0, 1.45.0)

## Table of Contents

- [Executive Summary](#executive-summary)
- [DataHub's Customization Architecture](#datahubs-customization-architecture)
- [Inventory of All Customizations](#inventory-of-all-customizations)
- [Upstream Changes Since 1.38.0](#upstream-changes-since-1380)
- [Conflict Analysis per Patched File](#conflict-analysis-per-patched-file)
- [Upstream Trimmers vs DataHub PathSpec](#upstream-trimmers-vs-datahub-pathspec)
- [CLL Consistency Gap](#cll-consistency-gap)
- [Recommended Migration Strategy](#recommended-migration-strategy)
- [Migration Effort Estimate](#migration-effort-estimate)
- [Known Bugs Fixed in This PR](#known-bugs-fixed-in-this-pr)

---

## Executive Summary

Upgrading from OpenLineage 1.38.0 to 1.45.0 is **medium-hard difficulty (3-5 days)** but
strategically valuable. The biggest opportunity is OpenLineage 1.39.0's **configurable dataset
name trimmers** — a heuristic partition-stripping system that overlaps with (but does not replace)
DataHub's PathSpec approach. Using both systems together would simplify DataHub's two most invasive
patches and fix a column-level lineage consistency gap.

**Key numbers:**
- 10 patched upstream files, 12 entirely new DataHub files
- 3 files with HIGH conflict risk (PlanUtils, PathUtils, RddPathUtils — 5 upstream commits each)
- 2 files with MEDIUM conflict risk (RemovePathPatternUtils, SparkOpenLineageExtensionVisitorWrapper)
- 5 files with LOW conflict risk (trivial to reapply)

---

## DataHub's Customization Architecture

DataHub maintains a **patch-based customization system** with versioned subdirectories:

```
patches/
└── datahub-customizations/
    └── v1.38.0/           # Patches specific to OpenLineage 1.38.0
        ├── Vendors.patch
        ├── PathUtils.patch
        ├── PlanUtils.patch
        ├── RemovePathPatternUtils.patch
        ├── RddPathUtils.patch
        ├── SparkOpenLineageExtensionVisitorWrapper.patch
        ├── WriteToDataSourceV2Visitor.patch
        ├── StreamingDataSourceV2RelationVisitor.patch
        ├── MergeIntoCommandInputDatasetBuilder.patch
        └── MergeIntoCommandEdgeInputDatasetBuilder.patch
```

Customized OpenLineage classes live at `src/main/java/io/openlineage/` and override the upstream
dependency via classpath ordering in the shadow JAR. The `io.openlineage` package is explicitly
excluded from relocation so these overrides take effect.

**Upgrade scripts:**
- `scripts/fetch-upstream.sh` — downloads upstream source files for a given version
- `scripts/generate-patches.sh` — diffs current source against upstream to create patches
- `scripts/upgrade-openlineage.sh` — automates the full upgrade workflow

> **Note:** The upgrade scripts currently have a bug where they look for patches in
> `patches/datahub-customizations/<name>.patch` (flat) instead of the versioned subdirectory
> `patches/datahub-customizations/v<version>/<name>.patch`. This should be fixed before using them
> for the 1.45.0 upgrade.

---

## Inventory of All Customizations

### Patched Files (modifications to upstream OpenLineage classes)

| # | File | Patch Size | Purpose | Upstreamable? |
|---|------|-----------|---------|:---:|
| 1 | **Vendors.java** | Minimal (3 lines) | Adds `RedshiftVendor` to vendor list | ✅ Yes |
| 2 | **PathUtils.java** | Minimal (5 lines) | Symlink for tables outside warehouse | ✅ Yes |
| 3 | **PlanUtils.java** | Major (30+ lines) | Replaces `getDirectoryPath()` with DataHub PathSpec resolution | ❌ DataHub-specific |
| 4 | **RemovePathPatternUtils.java** | Major (50+ lines) | Replaces regex path removal with DataHub PathSpec transformations | ❌ DataHub-specific |
| 5 | **RddPathUtils.java** | Minimal (3 lines) | Downgrades 3 `log.warn` to `log.debug` for noise reduction | ✅ Yes |
| 6 | **SparkOpenLineageExtensionVisitorWrapper.java** | Major (40+ lines) | Adapts reflection/classloader for DataHub's shading strategy | ⚠️ Maybe |
| 7 | **WriteToDataSourceV2Visitor.java** | Major (100+ lines) | Adds ForeachBatch and file-based streaming write support | ⚠️ Maybe |
| 8 | **StreamingDataSourceV2RelationVisitor.java** | Medium (10 lines) | Adds file-based streaming source detection | ✅ Yes |
| 9 | **MergeIntoCommandInputDatasetBuilder.java** | Major (25+ lines) | Recursive BFS for complex subqueries in Delta MERGE | ✅ Yes |
| 10 | **MergeIntoCommandEdgeInputDatasetBuilder.java** | Major (25+ lines) | Same as above for Databricks Edge variant | ⚠️ Maybe |

### Entirely New DataHub Files (no upstream equivalent)

| File | Purpose |
|------|---------|
| **RedshiftVendor.java** | Entry point for Redshift vendor support |
| **RedshiftDataset.java** | Creates datasets from Redshift JDBC URLs |
| **RedshiftRelationVisitor.java** | Visitor for LogicalRelations containing RedshiftRelation |
| **RedshiftVisitorFactory.java** | Factory providing Redshift visitors |
| **RedshiftEventHandlerFactory.java** | Factory for Redshift event handlers |
| **RedshiftSaveIntoDataSourceCommandBuilder.java** | Handles SaveIntoDataSourceCommand for Redshift |
| **Constants.java** | Redshift class name constants |
| **FileStreamMicroBatchStreamStrategy.java** | File-based streaming in micro-batch mode |
| **SaveIntoDataSourceCommandVisitor.java** | Handles Delta, JDBC, Kafka, Kusto writes |
| **JdbcSparkUtils.java** | JDBC SQL parsing, dialect mapping (sqlserver→mssql) |
| **VendorsImpl.java** | Vendors interface implementation |
| **VendorsContext.java** | Shared vendor context |

---

## Upstream Changes Since 1.38.0

### Changes per patched file

| File | Upstream Commits | Conflict Risk | Key PRs |
|------|:---:|:---:|---------|
| **PlanUtils.java** | 5 | 🔴 HIGH | #3985 (S3 optimization), #4283 (symlink extraction), #4331 (Iceberg schema) |
| **PathUtils.java** | 5 | 🔴 HIGH | #4283 (DataSourceRDD symlinks), #4057 (Databricks Unity facets) |
| **RddPathUtils.java** | 5 | 🟡 MEDIUM | #4263 (Iceberg DataSourceRDD), #4283/#4331 (symlink/schema) |
| **RemovePathPatternUtils.java** | 2 | 🟡 MEDIUM | #4228 (CLL facet cleanup) |
| **SparkOpenLineageExtensionVisitorWrapper.java** | 2 | 🟢 LOW | Copyright + minor |
| **WriteToDataSourceV2Visitor.java** | 1 | 🟢 LOW | Minimal |
| **StreamingDataSourceV2RelationVisitor.java** | 1 | 🟢 LOW | Minimal |
| **MergeIntoCommandInputDatasetBuilder.java** | 1 | 🟢 LOW | Likely copyright only |
| **MergeIntoCommandEdgeInputDatasetBuilder.java** | 1 | 🟢 LOW | Likely copyright only |
| **Vendors.java** | 1 | 🟢 LOW | Likely copyright only |

### Notable upstream features by version

**1.39.0 — Configurable dataset name trimmers (PR #3996)**
Automatic heuristic-based partition stripping. Strips date directories, key=value partitions,
year/month directories. Works without configuration. See [detailed comparison below](#upstream-trimmers-vs-datahub-pathspec).

**1.39.0 — S3 path resolution optimization (PR #3985)**
`PlanUtils.getDirectoryPath()` now skips redundant `getFileStatus` calls for S3 by checking if
parent dir was already added. Directly conflicts with DataHub's replacement of this method.

**1.42.1 — Path pattern removal in CLL facets (PR #4228)**
Extends `RemovePathPatternUtils` to also clean up `ColumnLineageFacet` field references. DataHub's
full replacement of this class would miss this improvement.

**1.42.1 — Disable RDD event emitting (PR #4118)**
New config `spark.openlineage.filter.rddEventsDisabled` to selectively disable OL events for RDD
operations. Useful for reducing noise from non-materializing RDD jobs.

**1.43.0 — Iceberg DataSourceRDD support (PR #4263)**
Adds support for extracting lineage from Iceberg's DataSourceRDD. Touches RddPathUtils.

**1.44.0 — DataSourceRDD symlink extraction (PR #4283)**
Adds input symlink support for DataSourceRDD across PathUtils, PlanUtils, and RddPathUtils.

**1.45.0 — Performance: identity-based VisitedNodes tracking (PR #4383)**
Replaces `semanticHash` with identity-based tracking in BFS traversal. Touches the same traversal
pattern DataHub uses in MergeInto builders.

**1.45.0 — HashSet for BFS visited set (PR #4376)**
Performance improvement using HashSet instead of LinkedList for BFS. Relevant to DataHub's
MergeInto builders which use a queue-based BFS.

---

## Conflict Analysis per Patched File

### PlanUtils.java — 🔴 HIGH RISK

**DataHub's change:** Replaces `getDirectoryPath(Path, Configuration)` entirely with a version
that reads `spark.datahub.*` config, creates a `SparkLineageConf`, and transforms paths via
`HdfsPathDataset.create()`. The original method is preserved as `getDirectoryPathOl()`.

**Upstream changes since 1.38.0:**
1. **PR #3985** — Optimizes the original `getDirectoryPath` to batch S3 `getFileStatus` calls.
   The method DataHub replaces was significantly rewritten.
2. **PR #4283** — Adds symlink extraction in `getDirectoryPaths` (the caller of
   `getDirectoryPath`). New parameters and return types may be involved.
3. **PR #4331** — Adds Iceberg schema extraction logic alongside path processing.

**Merge strategy:** DataHub's replacement method needs to be rebased on top of the new upstream
`getDirectoryPath` signature. The S3 optimization (#3985) is doing something similar to what
DataHub needs (avoiding per-file filesystem calls), which could simplify the DataHub override.

### PathUtils.java — 🔴 HIGH RISK

**DataHub's change:** Adds 5-line `else` branch in `fromCatalogTable()` for tables outside
warehouse — creates a symlink to actual location + tableName.

**Upstream changes since 1.38.0:**
1. **PR #4283** — Adds DataSourceRDD symlink extraction (new methods)
2. **PR #4057** — Adds Databricks Unity Catalog facets

**Merge strategy:** DataHub's change is minimal and localized. Should be easy to reapply if the
`fromCatalogTable()` method signature hasn't changed. Verify the surrounding code context.

### RddPathUtils.java — 🟡 MEDIUM RISK

**DataHub's change:** Three `log.warn` → `log.debug` changes for noise reduction.

**Upstream changes since 1.38.0:** 5 commits adding Iceberg support, DataSourceRDD extractors,
and symlink extraction. The file has changed significantly but DataHub's changes are trivial
log-level adjustments.

**Merge strategy:** Fetch new upstream, reapply 3 log-level changes. Trivial.

### RemovePathPatternUtils.java — 🟡 MEDIUM RISK

**DataHub's change:** Replaces both `removeOutputsPathPattern()` and `removeInputsPathPattern()`
with DataHub PathSpec-based implementations. Originals preserved as `*_ol()` suffixed methods.
Adds mutable static `sparkConf` field and `loadSparkConf()` method.

**Upstream changes since 1.38.0:**
1. **PR #4228** — Extends path pattern removal to `ColumnLineageFacet` references. This is
   important for CLL consistency and DataHub's replacement misses it entirely.

**Merge strategy:** If adopting upstream trimmers for basic partition stripping, DataHub's
replacement could be significantly simplified. The PathSpec-specific logic could be moved to a
DataHub-owned utility class rather than patching the upstream class.

### Remaining 6 files — 🟢 LOW RISK

All have 1-2 upstream commits (likely copyright updates). DataHub's patches should reapply
cleanly with minimal or no conflicts.

---

## Upstream Trimmers vs DataHub PathSpec

### How upstream trimmers work (PR #3996, released 1.39.0)

The upstream system is a **heuristic, automatic** approach. It operates on dataset names and
iteratively removes trailing path segments that look like partition components.

**Built-in trimmers (4 total, all enabled by default):**

| Trimmer | What it strips | Example |
|---|---|---|
| **DateTrimmer** | Directory containing a parseable date | `.../20250901/` → trimmed |
| **KeyValueTrimmer** | Directory with exactly one `=` sign | `.../year=2024/` → trimmed |
| **MultiDirDateTrimmer** | Multiple dirs forming yyyy/MM or yyyy/MM/dd | `.../2025/09/01/` → all trimmed |
| **YearMonthTrimmer** | Directory that is yyyy-MM or yyyyMM | `.../202509/` → trimmed |

**Configuration:**
```properties
spark.openlineage.dataset.disabledTrimmers=io.openlineage.client.dataset.DatasetNameTrimmer$DateTrimmer
spark.openlineage.dataset.extraTrimmers=com.example.MyCustomTrimmer
```

Custom trimmers can be added by implementing the `DatasetNameTrimmer` interface.

### How DataHub PathSpec works

DataHub uses **explicit pattern matching** with `{table}` glob markers:

```properties
spark.datahub.platform.s3.path_spec_list=s3a://bucket/warehouse/{table}
spark.datahub.platform.s3.env=PROD
spark.datahub.platform.s3.platformInstance=myInstance
```

Everything up to and including the `{table}` segment becomes the dataset name; everything after
is discarded. Supports `*` wildcards for directory matching.

### Capability comparison

| Capability | Upstream Trimmers | DataHub PathSpec |
|---|:---:|:---:|
| Strip `key=value` partitions | ✅ Auto | ✅ Needs config |
| Strip date directories | ✅ Auto | ✅ Needs config |
| Strip multi-dir dates (yyyy/MM/dd) | ✅ Auto | ✅ Needs config |
| Strip arbitrary filenames (file.parquet) | ❌ | ✅ |
| `{table}` glob patterns | ❌ | ✅ |
| Wildcard directory matching (`*`) | ❌ | ✅ |
| Arbitrary regex removal | ❌ | ✅ |
| Platform instance per path | ❌ | ✅ |
| Environment override per path | ❌ | ✅ |
| CLL field reference consistency | ✅ | ❌ (gap) |
| Dataset merging (SubsetDefinition facet) | ✅ | ❌ |
| Zero-config for common patterns | ✅ | ❌ |
| Custom Java trimmers | ✅ | N/A |

### Real-world scenario comparison

**Scenario 1: Hive-partitioned S3 path**
```
Input:  s3a://bucket/table/year=2024/month=01/day=15/part-00000.parquet
```
- **Upstream trimmers:** → `bucket/table/part-00000.parquet` (strips key=value, leaves filename) ⚠️
- **DataHub PathSpec** (`s3a://bucket/{table}`): → `bucket/table` ✅

**Scenario 2: Date-partitioned path**
```
Input:  s3a://bucket/events/20250901/data.json
```
- **Upstream trimmers:** → `bucket/events/data.json` (strips date, leaves filename) ⚠️
- **DataHub PathSpec** (`s3a://bucket/{table}`): → `bucket/events` ✅

**Scenario 3: Simple Hive partitions only**
```
Input:  hdfs:///warehouse/db/table/dt=2024-01-01/
```
- **Upstream trimmers:** → `warehouse/db/table` ✅
- **DataHub PathSpec** (`hdfs:///warehouse/db/{table}`): → `warehouse/db/table` ✅

**Conclusion:** The systems are **complementary, not replacements**. Upstream trimmers handle
common partition patterns automatically but leave filenames. PathSpec handles explicit mapping
with richer metadata assignment. Both are needed for full coverage.

---

## CLL Consistency Gap

This is a **significant bug** in DataHub's current approach that the upstream trimmers would fix.

### The problem

DataHub's `RemovePathPatternUtils` transforms dataset names on `InputDataset`/`OutputDataset`
objects. However, upstream OpenLineage's `ColumnLevelLineageBuilder` builds CLL field references
**before** DataHub's transformation runs. This means:

```
Dataset name (after PathSpec):  s3://bucket/my-table
CLL field reference:            urn:li:schemaField:(urn:li:dataset:(...,s3://bucket/my-table/year=2024/month=01,...),col1)
                                                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                                                      Points to UNTRIMMED name — no matching dataset!
```

The result: column-level lineage edges point to dataset URNs that don't match the actual dataset
entity. CLL appears broken in the DataHub UI.

### The upstream fix

Upstream trimmers are applied inside `ColumnLevelLineageBuilder.addInput()` via
`DatasetReducerUtils.trimDatasetIdentifier()`. This means CLL field references are **always
consistent** with the trimmed dataset name:

```
Dataset name (after trimmer):   s3://bucket/my-table
CLL field reference:            urn:li:schemaField:(urn:li:dataset:(...,s3://bucket/my-table,...),col1)
                                                                      ^^^^^^^^^^^^^^^^^^^^
                                                                      Matches! ✅
```

### Recommendation

Enable upstream trimmers AND apply PathSpec transformations at the same layer. Alternatively,
contribute a DataHub-specific trimmer that implements `DatasetNameTrimmer` and delegates to
PathSpec logic — this would automatically get CLL consistency for free.

---

## Recommended Migration Strategy

### Phase 1: Enable upstream trimmers alongside PathSpec (low effort)

1. Upgrade to 1.45.0 with all patches rebased
2. Enable upstream trimmers (they're on by default) — automatic partition stripping
3. Keep DataHub's PathSpec patches for explicit `{table}` mapping
4. Users get automatic partition stripping for common patterns without configuration

### Phase 2: Simplify PlanUtils and RemovePathPatternUtils patches (medium effort)

1. **PlanUtils.getDirectoryPath:** Instead of replacing the entire method, create a thin wrapper
   that applies PathSpec only when `spark.datahub.platform.*.path_spec_list` is configured,
   otherwise delegates to upstream's optimized version
2. **RemovePathPatternUtils:** Move DataHub's PathSpec logic to a new DataHub-owned class
   (e.g., `DatahubPathSpecUtils`) and use it as a post-processor rather than patching the
   upstream class. This eliminates the most invasive patch.
3. Incorporate upstream's PR #4228 CLL fix automatically (no longer overriding the methods)

### Phase 3: Implement DataHub trimmer (optional, highest value)

Implement `DatasetNameTrimmer` that delegates to PathSpec logic:
```java
public class DatahubPathSpecTrimmer implements DatasetNameTrimmer {
    @Override
    public Optional<String> trim(String datasetName) {
        // Read spark.datahub.* config, apply PathSpec matching
        // Return trimmed name if PathSpec matches, empty otherwise
    }
}
```

Configure via:
```properties
spark.openlineage.dataset.extraTrimmers=datahub.spark.trimmer.DatahubPathSpecTrimmer
```

**Benefits:**
- PathSpec transformation happens at the trimmer layer → automatic CLL consistency
- No more patching `PlanUtils.getDirectoryPath` or `RemovePathPatternUtils`
- Works with upstream's `DatasetReducer` for dataset merging
- Future OL upgrades become trivial (no patches on these files)

---

## Migration Effort Estimate

| Task | Effort | Difficulty |
|------|--------|-----------|
| Reapply 6 low-risk patches (Vendors, RddPathUtils, MergeInto x2, WriteToDataSourceV2, Streaming) | 0.5 day | Easy |
| Rebase PlanUtils.java against 5 upstream commits | 1-2 days | Hard |
| Rebase RemovePathPatternUtils.java against 2 upstream commits + decide on trimmer integration | 1 day | Medium |
| Rebase PathUtils.java against 5 upstream commits | 0.5 day | Medium (small patch) |
| Test all scenarios (partition stripping, PathSpec, CLL, streaming, MergeInto) | 1 day | Medium |
| **Total** | **3-5 days** | |

With Phase 3 (DataHub trimmer), add 1-2 days for implementation but **save time on every future upgrade** (eliminates 2 hardest patches permanently).

---

## Known Bugs Fixed in This PR

The following bugs were identified during review and fixed in commit `3a9ba52b4e`:

### 1. Missing output lineage edges (customer-reported)

**Root cause:** When coalesced emission fires on early Spark events (START), `DatahubJob.generateDataJobInputOutputMcp()` emitted a `dataJobInputOutput` UPSERT with empty `inputDatasetEdges` and `outputDatasetEdges` arrays. Later PATCH emissions could not reliably override this due to async processing ordering.

**Fix:** Skip `dataJobInputOutput` emission when all edge arrays are empty.

**File:** `DatahubJob.java` (openlineage-converter module)

### 2. SparkEnv NPE during shutdown

**Root cause:** `PlanUtils.getDirectoryPath()` calls `SparkEnv.get().conf()` without null-checking. `SparkEnv.get()` returns null during Spark shutdown.

**Fix:** Null-check and fall back to `getDirectoryPathOl()`.

**File:** `PlanUtils.java`

### 3. URISyntaxException crashes lineage pipeline

**Root cause:** `RemovePathPatternUtils.removePathPattern()` catches `InstantiationException` gracefully but wraps `URISyntaxException` in `RuntimeException`, crashing the entire pipeline for datasets with URI-unsafe names.

**Fix:** Handle both exception types the same way (log + return original name).

**File:** `RemovePathPatternUtils.java`

### 4. Double-Optional wrapping in getPattern()

**Root cause:** `Optional.of(context.getSparkContext())` wraps the returned `Optional<SparkContext>` in another `Optional`, then `.map()` calls `.get()` on the inner Optional — throwing `NoSuchElementException` when SparkContext is absent.

**Fix:** Use `context.getSparkContext()` directly.

**File:** `RemovePathPatternUtils.java`

### 5. Unguarded JDBC option access

**Root cause:** `SaveIntoDataSourceCommandVisitor` calls `.get()` on Scala `Option` for `dbtable` and `url` without checking `isDefined()`, throwing `NoSuchElementException` for query-based JDBC sinks.

**Fix:** Check `isDefined()` before `.get()`.

**File:** `SaveIntoDataSourceCommandVisitor.java`

### 6. Double build() on patch builder

**Root cause:** `DataJobInputOutputPatchBuilder.build()` was called twice — once for logging, once for the MCPs list. The class documents `getPathValues()` as "not idempotent."

**Fix:** Reuse the first built MCP.

**File:** `DatahubJob.java`

### 7. Per-dataset log.info flooding

**Root cause:** `RemovePathPatternUtils.removePathPattern()` logs at INFO for every dataset in every event, flooding production logs.

**Fix:** Downgrade to `log.debug`.

**File:** `RemovePathPatternUtils.java`
