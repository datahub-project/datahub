# Optional PySpark Support for S3 Source

DataHub's S3 source now supports optional PySpark installation through the `s3-slim` variant. This allows users to choose a lightweight installation when data lake profiling is not needed.

## Overview

The S3 source includes PySpark by default for backward compatibility and profiling support. For users who only need metadata extraction without profiling, the `s3-slim` variant provides a ~500MB smaller installation.

**Current implementation status:**

- ✅ **S3**: SparkProfiler pattern fully implemented (optional PySpark)
- **ABS**: Not yet implemented (still requires PySpark for profiling)
- **Unity Catalog**: Not affected by this change (uses separate profiling mechanisms)
- **GCS**: Does not support profiling

> **Note:** This change implements the SparkProfiler pattern for S3 only. The same pattern can be applied to other sources (ABS, etc.) in future PRs.

## PySpark Version

> **Current Version:** PySpark 3.5.x (3.5.6)
>
> PySpark 4.0 support is planned for a future release. Until then, all DataHub components use PySpark 3.5.x for compatibility and stability.

## Installation Options

### Standard Installation (includes PySpark)

```bash
pip install 'acryl-datahub[s3]'         # S3 with PySpark/profiling support
```

### Lightweight Installation (without PySpark)

For installations where you don't need profiling capabilities and want to save ~500MB:

```bash
pip install 'acryl-datahub[s3-slim]'    # S3 without profiling (~500MB smaller)
```

**Recommendation:** Use `s3-slim` when profiling is not needed.

The `data-lake-profiling` dependencies (included in standard `s3` by default):

- `pyspark~=3.5.6`
- `pydeequ>=1.1.0`
- Profiling dependencies (cachetools)

> **Note:** In a future major release (e.g., DataHub 2.0), the `s3-slim` variant may become the default, and PySpark will be truly optional. This current approach provides backward compatibility while giving users time to adapt.

### What's Included

**S3 source:**

Standard `s3` extra:

- ✅ Metadata extraction (schemas, tables, file listing)
- ✅ Data format detection (Parquet, Avro, CSV, JSON, etc.)
- ✅ Schema inference from files
- ✅ Table and column-level metadata
- ✅ Tags and properties extraction
- ✅ Data profiling (min/max, nulls, distinct counts)
- ✅ Data quality checks (PyDeequ-based)
- Includes: PySpark 3.5.6 + PyDeequ

`s3-slim` variant:

- ✅ All metadata features (same as above)
- ❌ Data profiling disabled
- No PySpark dependencies (~500MB smaller)

## Feature Comparison

| Feature                  | `s3-slim`        | Standard `s3`              |
| ------------------------ | ---------------- | -------------------------- |
| **Metadata extraction**  | ✅ Full support  | ✅ Full support            |
| **Schema inference**     | ✅ Full support  | ✅ Full support            |
| **Tags & properties**    | ✅ Full support  | ✅ Full support            |
| **Data profiling**       | ❌ Not available | ✅ Full profiling          |
| **Installation size**    | ~200MB           | ~700MB                     |
| **Install time**         | Fast             | Slower (PySpark build)     |
| **PySpark dependencies** | ❌ None          | ✅ PySpark 3.5.6 + PyDeequ |

## Configuration

### With Standard Installation (PySpark included)

When you install `acryl-datahub[s3]`, profiling works out of the box:

```yaml
source:
  type: s3
  config:
    path_specs:
      - include: s3://my-bucket/data/**/*.parquet
    profiling:
      enabled: true # Works seamlessly with standard installation
      profile_table_level_only: false
```

### With Slim Installation (no PySpark)

When you install `s3-slim`, disable profiling in your config:

```yaml
source:
  type: s3
  config:
    path_specs:
      - include: s3://my-bucket/data/**/*.parquet
    profiling:
      enabled: false # Required for s3-slim installation
```

**If you enable profiling with s3-slim installation**, you'll see a clear error message at runtime:

```
RuntimeError: PySpark is not installed, but is required for S3 profiling.
Please install with: pip install 'acryl-datahub[s3]'
```

## Developer Guide

### Implementation Pattern

The S3 source demonstrates the recommended pattern for isolating PySpark-dependent code. This pattern can be applied to ABS and other sources in future PRs.

**Architecture (currently implemented for S3 only):**

1. **Main source class** (`source.py`) - Contains no PySpark imports at module level
2. **Profiler class** (`profiling.py`) - Encapsulates all PySpark/PyDeequ logic in `SparkProfiler` class
3. **Conditional instantiation** - `SparkProfiler` created only when profiling is enabled
4. **TYPE_CHECKING imports** - Type annotations use TYPE_CHECKING block for optional dependencies

**Key Benefits:**

- ✅ Type safety preserved (mypy passes without issues)
- ✅ Proper code layer separation
- ✅ Works with both standard and `-slim` installations
- ✅ Clear error messages when dependencies missing
- ✅ Pattern can be reused for ABS and other sources

**Example structure:**

```python
# source.py
if TYPE_CHECKING:
    from datahub.ingestion.source.s3.profiling import SparkProfiler

class S3Source:
    profiler: Optional["SparkProfiler"]

    def __init__(self, config, ctx):
        if config.is_profiling_enabled():
            from datahub.ingestion.source.s3.profiling import SparkProfiler
            self.profiler = SparkProfiler(...)
        else:
            self.profiler = None
```

```python
# profiling.py
class SparkProfiler:
    """Encapsulates all PySpark/PyDeequ profiling logic."""

    def init_spark(self) -> Any:
        # Spark session initialization

    def read_file_spark(self, file: str, ext: str):
        # File reading with Spark

    def get_table_profile(self, table_data, dataset_urn):
        # Table profiling coordination
```

For more details, see the [Adding a Metadata Ingestion Source](../metadata-ingestion/adding-source.md#31-using-optional-dependencies-eg-pyspark) guide.

## Troubleshooting

### Error: "PySpark is not installed, but is required for profiling"

**Problem:** You installed a `-slim` variant but have profiling enabled in your config.

**Solutions:**

1. **Recommended:** Use standard installation with PySpark:

   ```bash
   pip uninstall acryl-datahub
   pip install 'acryl-datahub[s3]'    # For S3 profiling
   ```

2. **Alternative:** Disable profiling in your recipe:
   ```yaml
   profiling:
     enabled: false
   ```

### Verifying Installation

Check if PySpark is installed:

```bash
# Check installed packages
pip list | grep pyspark

# Test import in Python
python -c "import pyspark; print(pyspark.__version__)"
```

Expected output:

- Standard installation (`s3`): Shows `pyspark 3.5.x`
- Slim installation (`s3-slim`): Import fails or package not found

## Migration Guide

### Upgrading from Previous Versions

**No action required!** This change is fully backward compatible:

```bash
# Existing installations continue to work exactly as before
pip install 'acryl-datahub[s3]'  # Still includes PySpark by default (profiling supported)
```

**Recommended: Optimize installations**

- **S3 with profiling:** Keep using `acryl-datahub[s3]` (includes PySpark)
- **S3 without profiling:** Switch to `acryl-datahub[s3-slim]` to save ~500MB

```bash
# Recommended installations
pip install 'acryl-datahub[s3]'         # S3 with profiling support
pip install 'acryl-datahub[s3-slim]'    # S3 metadata only (no profiling)
```

### No Breaking Changes

This implementation maintains full backward compatibility:

- Standard `s3` extra includes PySpark (unchanged behavior)
- All existing recipes and configs continue to work
- New `s3-slim` variant available for users who want smaller installations
- Future DataHub 2.0 may flip defaults, but provides migration path

## Benefits for DataHub Actions

[DataHub Actions](https://github.com/datahub-project/datahub/tree/master/datahub-actions) depends on `acryl-datahub` and can benefit from `s3-slim` when profiling is not needed:

### Reduced Installation Size

DataHub Actions typically doesn't need data lake profiling capabilities since it focuses on reacting to metadata events, not extracting metadata from data lakes. Use `s3-slim` to reduce footprint:

```bash
# If Actions needs S3 metadata access but not profiling
pip install acryl-datahub-actions
pip install 'acryl-datahub[s3-slim]'
# Result: ~500MB smaller than standard s3 extra

# If Actions needs full S3 with profiling
pip install acryl-datahub-actions
pip install 'acryl-datahub[s3]'
# Result: Includes PySpark for profiling capabilities
```

### Faster Deployment

Actions services using `s3-slim` deploy faster in containerized environments:

- **Faster pip install**: No PySpark compilation required
- **Smaller Docker images**: Reduced base image size
- **Quicker cold starts**: Less code to load and initialize

### Fewer Dependency Conflicts

Actions workflows often integrate with other tools (Slack, Teams, email services). Using `s3-slim` reduces:

- Python version constraint conflicts
- Java/Spark runtime conflicts in restricted environments
- Transitive dependency version mismatches

### When Actions Needs Profiling

If your Actions workflow needs to trigger data lake profiling jobs, use the standard extra:

```bash
# Actions with data lake profiling capability
pip install 'acryl-datahub-actions'
pip install 'acryl-datahub[s3]'  # Includes PySpark by default
```

**Common Actions use cases that DON'T need PySpark:**

- Slack notifications on schema changes
- Propagating tags and terms to downstream systems
- Triggering dbt runs on metadata updates
- Sending emails on data quality failures
- Creating Jira tickets for governance issues
- Updating external catalogs (e.g., Alation, Collibra)

**Rare Actions use cases that MIGHT need PySpark:**

- Custom actions that programmatically trigger S3 profiling
- Actions that directly process data lake files (not typical)

## Benefits Summary

✅ **Backward compatible**: Standard `s3` extra unchanged, existing users unaffected
✅ **Smaller installations**: Save ~500MB with `s3-slim`
✅ **Faster setup**: No PySpark compilation with `s3-slim`
✅ **Flexible deployment**: Choose based on profiling needs
✅ **Type safety maintained**: Refactored with proper code layer separation (mypy passes)
✅ **Clear error messages**: Runtime errors guide users to correct installation
✅ **Actions-friendly**: DataHub Actions benefits from reduced footprint with `s3-slim`

**Key Takeaways:**

- Use `s3` if you need S3 profiling, `s3-slim` if you don't
- Pattern can be applied to other sources (ABS, etc.) in future PRs
- Existing installations continue working without changes
