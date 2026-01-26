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
ext.openLineageVersion = '1.38.0'
```

**Last upgraded:** October 2, 2025 (from 1.33.0 to 1.38.0)

**Key changes in 1.38.0:**

- ✅ AWS Glue ARN handling now in upstream (was DataHub customization)
- ✅ `getMetastoreUri()` and `getWarehouseLocation()` now public upstream
- ✅ SaveIntoDataSourceCommandVisitor Delta table handling adopted upstream
- ⚡ Enhanced schema handling with nested structs, maps, and arrays
- ⚡ New UnionRDD and NewHadoopRDD extractors for improved path detection

### Architecture: Patch-Based Customization System

DataHub maintains customizations to OpenLineage through a **version-organized patch-based system**:

```
patches/
└── datahub-customizations/      # Versioned patch files (committed to git)
    ├── v1.38.0/                 # Patches for OpenLineage 1.38.0
    │   ├── Vendors.patch
    │   ├── PathUtils.patch
    │   └── ...
    └── v1.33.0/                 # Patches for OpenLineage 1.33.0 (historical)
        └── ...

# Temporary files (NOT committed, downloaded when patching):
patches/upstream-<version>/      # Original OpenLineage files
patches/backup-<timestamp>/      # Automatic backups during upgrades
```

**Important**: Only `patches/datahub-customizations/` is version controlled. Each OpenLineage version has its own subdirectory (e.g., `v1.38.0/`) containing patches specific to that version. Upstream and backup files are temporary and excluded via `.gitignore`.

### Shadowed Classes Location

Custom OpenLineage implementations are in:

```
src/main/java/io/openlineage/
```

These files are OpenLineage source files with DataHub-specific modifications applied via patches.

### Known Customizations (v1.38.0)

Tracked via patch files in `patches/datahub-customizations/v1.38.0/`:

1. **`Vendors.patch`**: Adds `RedshiftVendor` to the vendors list
2. **`PathUtils.patch`**: "Table outside warehouse" symlink handling
3. **`PlanUtils.patch`**: Custom directory path handling with DataHub's HdfsPathDataset
4. **`RemovePathPatternUtils.patch`**: DataHub-specific PathSpec transformations
5. **`StreamingDataSourceV2RelationVisitor.patch`**: File-based streaming source support
6. **`WriteToDataSourceV2Visitor.patch`**: ForeachBatch streaming write support
7. **`MergeIntoCommandEdgeInputDatasetBuilder.patch`**: Delta Lake merge command complex subquery handling
8. **`MergeIntoCommandInputDatasetBuilder.patch`**: Enables recursive traversal for merge command subqueries
9. **`SparkOpenLineageExtensionVisitorWrapper.patch`**: Extension visitor customizations
10. **`RddPathUtils.patch`**: Debug log level for noise reduction (3 log statements changed from warn to debug)
11. **Redshift vendor** (`spark/agent/vendor/redshift/*`): Complete custom implementation (no upstream equivalent)

### Automated Upgrade Process

Use the automated upgrade script:

```bash
# Quick upgrade (recommended)
./scripts/upgrade-openlineage.sh 1.33.0 1.38.0

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
   ./scripts/fetch-upstream.sh 1.38.0
   ```

2. **Compare upstream changes** (optional):

   ```bash
   # See what changed between versions
   diff -r patches/upstream-1.33.0 patches/upstream-1.38.0
   ```

3. **Update source files**:

   ```bash
   # Copy new upstream files
   cp -r patches/upstream-1.38.0/* src/main/java/io/openlineage/
   ```

4. **Apply DataHub customizations**:

   ```bash
   # Apply all patches for the target version
   for patch in patches/datahub-customizations/v1.38.0/*.patch; do
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
   ./scripts/generate-patches.sh 1.38.0
   ```

7. **Update build.gradle**:

   ```gradle
   ext.openLineageVersion = '1.38.0'
   ```

8. **Test thoroughly**:
   ```bash
   ./gradlew :metadata-integration:java:acryl-spark-lineage:build
   ./gradlew :metadata-integration:java:acryl-spark-lineage:test
   ```

### Understanding Patch Files

Patch files show exactly what DataHub customized:

```bash
# View a specific customization for v1.38.0
cat patches/datahub-customizations/v1.38.0/Vendors.patch

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
patch -p0 --dry-run < patches/datahub-customizations/v1.38.0/Vendors.patch

# See what a patch would change
patch -p0 --dry-run < patches/datahub-customizations/v1.38.0/Vendors.patch | less
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
