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

**Known gap — column-level lineage (CLL) can point at untrimmed URNs.** DataHub's
`RemovePathPatternUtils` rewrites dataset names on the `Input`/`OutputDataset`, but upstream builds
CLL field references in `ColumnLevelLineageBuilder` *before* that rewrite runs — so CLL
`schemaField` URNs can reference the **untrimmed** dataset name and fail to match the emitted dataset
entity (CLL looks broken in the UI). Upstream trimmers avoid this because they run inside
`ColumnLevelLineageBuilder` (`DatasetReducerUtils.trimDatasetIdentifier`). The strategic fix is to
apply PathSpec at the trimmer layer (a DataHub `DatasetNameTrimmer`) so CLL consistency comes for
free and the two hardest patches (`PlanUtils`, `RemovePathPatternUtils`) can eventually be dropped.
See ING-2959 for the phased plan.

### Automated Upgrade Process

Use the automated upgrade script:

```bash
# Quick upgrade (recommended)
./scripts/upgrade-openlineage.sh 1.45.0 1.50.0

# This will:
# 1. Fetch new upstream files from GitHub
# 2. Compare old vs new upstream versions
# 3. Update shadowed files with new upstream code
# 4. Apply DataHub customizations via patches
# 5. Update build.gradle version
# 6. Report any conflicts requiring manual merge
```

### Manual Upgrade Steps

If you prefer manual control or need to resolve conflicts:

1. **Fetch upstream files**:

   ```bash
   ./scripts/fetch-upstream.sh 1.50.0
   ```

2. **Compare upstream changes** (optional):

   ```bash
   # See what changed between versions
   diff -r patches/upstream-1.45.0 patches/upstream-1.50.0
   ```

3. **Update source files**:

   ```bash
   # Copy new upstream files
   cp -r patches/upstream-1.50.0/* src/main/java/io/openlineage/
   ```

4. **Apply DataHub customizations**:

   ```bash
   # Apply all patches for the target version
   for patch in patches/datahub-customizations/v1.50.0/*.patch; do
     echo "Applying $(basename $patch)..."
     patch -p0 < "$patch" || echo "Conflict in $patch - manual merge required"
   done
   ```

5. **Handle conflicts**:

   - If patches fail, manually merge changes from:
     - Backup: `patches/backup-<timestamp>/`
     - New upstream: `patches/upstream-<new-version>/`
     - Patches show what to customize: `patches/datahub-customizations/v<version>/`

6. **Regenerate patches** (if you manually merged):

   ```bash
   ./scripts/generate-patches.sh 1.50.0
   ```

7. **Update build.gradle**:

   ```gradle
   ext.openLineageVersion = '1.50.0'
   ```

8. **Test thoroughly**:
   ```bash
   ./gradlew :metadata-integration:java:acryl-spark-lineage:build
   ./gradlew :metadata-integration:java:acryl-spark-lineage:test
   ```

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
