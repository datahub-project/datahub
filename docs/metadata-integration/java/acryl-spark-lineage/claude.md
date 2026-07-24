# acryl-spark-lineage Module

This module integrates OpenLineage with DataHub's Spark lineage collection. It contains shadowed and modified OpenLineage classes to support custom functionality.

## Architecture

This module:

- Builds shadow JARs for multiple Scala versions (2.12 and 2.13)
- Contains custom OpenLineage class implementations
- Depends on `io.openlineage:openlineage-spark` as specified by `ext.openLineageVersion` in the root `build.gradle`

## OpenLineage Version Upgrade Process

### Current Version

The OpenLineage version is defined in the root `build.gradle`:

```gradle
ext.openLineageVersion = '1.50.0'
```

**Last upgraded:** July 3, 2026 (from 1.45.0 to 1.50.0)

**Key changes across 1.46.0 → 1.50.0:**

- ✅ Iceberg-on-Glue symlink now detected via Spark config upstream (1.46, #4384) — the
  temporary DataHub `AwsUtils` backport was removed as a result
- ✅ `DatasetFactory.getDataset(...)` overloads replaced by the `sparkDatasetBuilder()` builder
  pattern (1.48); all vendored call sites migrated
- ✅ `BaseCatalogTypeHandler.getIdentifier()` now returns `Optional<DatasetIdentifier>` (1.48)
- ⚡ BigLake / GCP lakehouse Hive catalog symlink support (1.46) and S3 default-location fix (1.47)
- ⚡ SQL Server JDBC → `MsSqlDialect`; S3 Tables + Snowflake Horizon Iceberg REST catalogs (1.48)
- ⚡ `PartitionedFile.filePath()` returns `SparkPath` and `DataSourceRDDPartition.inputPartition()`
  became `inputPartitions()` — older-Spark branches now access these reflectively
- 🔒 httpclient5 → 5.6.1 and AWS SDK CVE bumps (1.47)

### Architecture: Patch-Based Customization System

DataHub maintains customizations to OpenLineage through a **version-organized patch-based system**:

```
patches/
└── datahub-customizations/      # Versioned patch files (committed to git)
    ├── v1.50.0/                 # Patches for OpenLineage 1.50.0 (current)
    │   ├── Vendors.patch
    │   ├── PathUtils.patch
    │   └── ...
    ├── v1.45.0/                 # Patches for OpenLineage 1.45.0 (historical)
    ├── v1.38.0/                 # Patches for OpenLineage 1.38.0 (historical)
    └── v1.33.0/                 # Patches for OpenLineage 1.33.0 (historical)
        └── ...

# Temporary files (NOT committed, downloaded when patching):
patches/upstream-<version>/      # Original OpenLineage files
patches/backup-<timestamp>/      # Automatic backups during upgrades
```

**Important**: Only `patches/datahub-customizations/` is version controlled. Each OpenLineage version has its own subdirectory (e.g., `v1.50.0/`) containing patches specific to that version. Upstream and backup files are temporary and excluded via `.gitignore`.

### Shadowed Classes Location

Custom OpenLineage implementations are in:

```
src/main/java/io/openlineage/
```

These files are OpenLineage source files with DataHub-specific modifications applied via patches.

### Known Customizations (v1.50.0)

Tracked via patch files in `patches/datahub-customizations/v1.50.0/`:

1. **`Vendors.patch`**: Adds `RedshiftVendor` to the vendors list
2. **`PathUtils.patch`**: "Table outside warehouse" symlink handling
3. **`PlanUtils.patch`**: Custom directory path handling with DataHub's HdfsPathDataset
4. **`RemovePathPatternUtils.patch`**: DataHub-specific PathSpec transformations
5. **`SaveIntoDataSourceCommandVisitor.patch`**: JDBC / Delta write output-dataset handling
6. **`StreamingDataSourceV2RelationVisitor.patch`**: File-based streaming source support
7. **`WriteToDataSourceV2Visitor.patch`**: ForeachBatch streaming write support
8. **`MergeIntoCommandEdgeInputDatasetBuilder.patch`**: Delta Lake merge command complex subquery handling
9. **`MergeIntoCommandInputDatasetBuilder.patch`**: Enables recursive traversal for merge command subqueries
10. **`SparkOpenLineageExtensionVisitorWrapper.patch`**: Extension visitor customizations

**DataHub-specific files (not patch-tracked — no upstream equivalent to diff against):**

- **`RddDatasetInfoExtractor`** (`spark/agent/util/`): a fork of upstream's former `RddPathUtils`,
  renamed and heavily rewritten; upstream no longer ships `RddPathUtils` at the tracked path, so it
  is maintained as a standalone DataHub file rather than a patch.
- **`JdbcSparkUtils`**, **`FileStreamMicroBatchStreamStrategy`**: DataHub additions.
- **Redshift vendor** (`spark/agent/vendor/redshift/*`): Complete custom implementation.

### Relationship to upstream trimmers & the CLL consistency gap

Upstream OpenLineage (since 1.39, PR #3996) ships **dataset name trimmers** — heuristic,
on-by-default strippers for common partition patterns (`key=value`, date dirs, `yyyy/MM/dd`,
`yyyyMM`), configurable via `spark.openlineage.dataset.disabledTrimmers` / `.extraTrimmers`.
DataHub's **PathSpec** (`spark.datahub.platform.<p>.path_spec_list` with `{table}` markers) is a
different mechanism: explicit pattern matching that also assigns platform-instance/env per path and
strips arbitrary filenames/regex.

They are **complementary, not replacements**: upstream trimmers auto-handle partition dirs but leave
the trailing filename; PathSpec does explicit `{table}` mapping with richer metadata. Full coverage
generally wants both.

**Default behavior in this plugin (`DatahubSparkListener.configureDatasetTrimmers`).** The upstream
trimmers run _before_ DataHub's path handling, so with a DataHub path-trimming mechanism configured
they double-transform names and can break patterns that reference partition segments. Therefore the
plugin **disables the OpenLineage trimmers when DataHub already does its own path trimming** — i.e.
when a `path_spec_list` or a `file_partition_regexp` is set — and otherwise leaves them on (upstream
default). Override explicitly with `spark.datahub.metadata.dataset.openLineageTrimmersEnabled`
(`true` forces them on, `false` forces them off); a directly-set
`spark.openlineage.dataset.disabledTrimmers` always wins.

**Known gap — column-level lineage (CLL) can point at untrimmed URNs.** DataHub's
`RemovePathPatternUtils` rewrites dataset names on the `Input`/`OutputDataset`, but upstream builds
CLL field references in `ColumnLevelLineageBuilder` _before_ that rewrite runs — so CLL
`schemaField` URNs can reference the **untrimmed** dataset name and fail to match the emitted dataset
entity (CLL looks broken in the UI). Upstream trimmers avoid this because they run inside
`ColumnLevelLineageBuilder` (`DatasetReducerUtils.trimDatasetIdentifier`). The strategic fix is to
apply PathSpec at the trimmer layer (a DataHub `DatasetNameTrimmer`) so CLL consistency comes for
free and the two hardest patches (`PlanUtils`, `RemovePathPatternUtils`) can eventually be dropped.
See ING-2959 for the phased plan.

### Upgrade Process (3-way merge — the reliable path)

The 1.33→1.38→1.45→1.50 upgrades showed that the only robust way to move a **patch-tracked**
vendored file forward is a **3-way merge**: replay upstream's `old→new` delta on top of our
customized copy. Blind `cp new-upstream + patch -p0` (what `upgrade-openlineage.sh` attempts) breaks
whenever upstream changed a patched file — e.g. OL 1.48's `DatasetFactory` builder refactor rewrote
call sites in several patched files. Prefer the steps below; treat `upgrade-openlineage.sh` as an
optional first pass only.

1. **Fetch both the old and new upstream** (the old version is the merge base):

   ```bash
   ./scripts/fetch-upstream.sh <old-version>   # e.g. 1.45.0
   ./scripts/fetch-upstream.sh <new-version>   # e.g. 1.50.0
   ```

2. **3-way merge each patch-tracked file** (see "Known Customizations" for the list). `git merge-file`
   applies the upstream `old→new` delta onto our current customized file, marking only real overlaps:

   ```bash
   f=spark/agent/util/PathUtils.java   # repeat per patched file
   git merge-file -p \
     src/main/java/io/openlineage/$f \
     patches/upstream-<old-version>/$f \
     patches/upstream-<new-version>/$f > /tmp/merged && mv /tmp/merged src/main/java/io/openlineage/$f
   ```

   Files upstream didn't touch merge cleanly (empty delta). Resolve any conflict markers by hand.

3. **Port the DataHub-specific files by hand** — `RddDatasetInfoExtractor`, `JdbcSparkUtils`,
   `FileStreamMicroBatchStreamStrategy`, and the redshift vendor have **no upstream** to merge
   against, so adapt them directly to any changed OL/Spark APIs (see step 6).

4. **Bump the version** in the root `build.gradle`:

   ```gradle
   ext.openLineageVersion = '<new-version>'
   ```

5. **Regenerate lockfiles**, then **verify Spotless entries survived**:

   ```bash
   ./gradlew resolveAndLockAll --write-locks
   ```

   ⚠️ Some environments run `resolveAndLockAll` without realizing Spotless's dynamically-named
   config, silently dropping its locked deps (`google-java-format`, `guava`, `error_prone`, …). After
   regenerating, `git diff` the lockfiles: the only changes should be the genuine OL dependency
   updates. If the `spotless<hash>` entries disappeared, restore them from the pre-change lockfiles.

6. **Compile-driven fix of breaking API changes** — compile, fix what breaks, repeat:

   ```bash
   ./gradlew :metadata-integration:java:openlineage-converter:compileJava \
             :metadata-integration:java:acryl-spark-lineage:compileTestJava
   ```

7. **Drop backports upstream now ships natively.** Check the OL changelog for the intervening
   versions and remove any DataHub workaround that became redundant (e.g. the `AwsUtils` Glue-ARN
   backport was removed once OL 1.46 shipped `isIcebergUsingGlue`).

8. **Regenerate the customization patches**:

   ```bash
   ./scripts/generate-patches.sh <new-version>
   ```

9. **Run the tests** (see below) — including the real-Spark and Spark 4 smoke suites.

### Testing an upgrade

```bash
# Unit tests + shaded-jar leak check (fast, no Docker)
./gradlew :metadata-integration:java:acryl-spark-lineage:test \
          :metadata-integration:java:acryl-spark-lineage:checkShadowJar
./gradlew :metadata-integration:java:openlineage-converter:test

# Real-Spark 3.5 (Scala 2.12) smoke suite — requires Docker (testcontainers/moto)
./gradlew :metadata-integration:java:acryl-spark-lineage:sparkRealSmokeTest

# Apache Spark 4.x (Scala 2.13) compatibility — requires Docker + JDK 17+; runs against the
# shaded 2.13 agent jar (see the Spark 4 note below)
./gradlew :metadata-integration:java:acryl-spark-lineage:sparkSmoke4Test
```

The `sparkRealSmokeTest` and `sparkSmoke4Test` tasks are also run by the `spark-smoke-test` CI
workflow, so an upgrade that passes them locally is what CI will verify.

### Spark 4.x compatibility (`sparkSmoke4Test`)

Spark 4 is **Scala 2.13 + Java 17 only**, so its smoke test lives in its own `src/sparkSmoke4`
source set and runs against the real shaded `shadowJar_2_13` jar (where our bundled deps are
relocated under `io.acryl.shaded`, avoiding clashes with Spark 4's own antlr/jackson). It forces
`antlr4-runtime` to 4.13.x for Spark 4's `SqlBaseLexer` (the repo otherwise pins 4.9.3) and is
excluded from dependency locking (its Spark 4 deps are test-only, not part of the shipped artifact).

### Understanding Patch Files

Patch files show exactly what DataHub customized:

```bash
# View a specific customization for v1.50.0
cat patches/datahub-customizations/v1.50.0/Vendors.patch

# Example output shows:
# - Lines removed from upstream (-)
# - Lines added by DataHub (+)
# - Context around changes
```

### Adding New Customizations

If you need to customize additional files:

1. Make your changes to files in `src/main/java/io/openlineage/`
2. Regenerate patches:
   ```bash
   ./scripts/generate-patches.sh <current-version>
   ```
3. The script will update `patches/datahub-customizations/` with your new changes

### Troubleshooting

**Patch conflicts during upgrade:**

- The upgrade script preserves backups in `patches/backup-<timestamp>/`
- Manually merge by comparing:
  - Your backup (shows DataHub customizations)
  - New upstream (shows OpenLineage changes)
  - Existing patch (shows what customizations to preserve)

**Files that are entirely DataHub-specific:**

- `FileStreamMicroBatchStreamStrategy.java` - Custom file-based streaming
- Redshift vendor files - Complete custom Redshift support
- These have `.note` files instead of `.patch` files

**Debugging patches:**

```bash
# Dry-run to see if patch will apply cleanly
patch -p0 --dry-run < patches/datahub-customizations/v1.50.0/Vendors.patch

# See what a patch would change
patch -p0 --dry-run < patches/datahub-customizations/v1.50.0/Vendors.patch | less
```

### Debugging

To see resolved dependencies for each Scala version:

```bash
./gradlew :metadata-integration:java:acryl-spark-lineage:debugDependencies
```

## Build Tasks

**Building from repository root:**

```bash
# Build all shadow JARs (both Scala 2.12 and 2.13)
./gradlew -PjavaClassVersionDefault=8 :metadata-integration:java:acryl-spark-lineage:shadowJar

# Build specific Scala version
./gradlew -PjavaClassVersionDefault=8 :metadata-integration:java:acryl-spark-lineage:shadowJar_2_12
./gradlew -PjavaClassVersionDefault=8 :metadata-integration:java:acryl-spark-lineage:shadowJar_2_13

# Run tests
./gradlew :metadata-integration:java:acryl-spark-lineage:test

# Run integration tests
./gradlew :metadata-integration:java:acryl-spark-lineage:integrationTest

# Full build with tests
./gradlew :metadata-integration:java:acryl-spark-lineage:build
```

**Note:** Java 8 is required to build this project, specified via `-PjavaClassVersionDefault=8`. The shadow JARs are output to `build/libs/` with filenames like:

- `acryl-spark-lineage_2.12-<version>.jar`
- `acryl-spark-lineage_2.13-<version>.jar`



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-integration](https://github.com/datahub-project/datahub/tree/master/metadata-integration) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
