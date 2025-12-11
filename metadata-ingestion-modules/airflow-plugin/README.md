# Datahub Airflow Plugin

See [the DataHub Airflow docs](https://docs.datahub.com/docs/lineage/airflow) for details.

## Version Compatibility

The plugin supports Apache Airflow versions 2.7+ and 3.1+.

| Airflow Version | Extra to Install | Status                 | Notes                            |
| --------------- | ---------------- | ---------------------- | -------------------------------- |
| 2.7-2.10        | `[airflow2]`     | ✅ Fully Supported     |                                  |
| 3.0.x           | `[airflow3]`     | ⚠️ Requires manual fix | Needs `pydantic>=2.11.8` upgrade |
| 3.1+            | `[airflow3]`     | ✅ Fully Supported     |                                  |

**Note on Airflow 3.0.x**: Airflow 3.0.6 pins pydantic==2.11.7, which contains a bug that prevents the DataHub plugin from importing correctly. This issue is resolved in Airflow 3.1.0+ which uses pydantic>=2.11.8. If you must use Airflow 3.0.6, you can manually upgrade pydantic to >=2.11.8, though this may conflict with Airflow's dependency constraints. We recommend upgrading to Airflow 3.1.0 or later.

Related issue: https://github.com/pydantic/pydantic/issues/10963

## Installation

The installation command varies depending on your Airflow version due to different OpenLineage dependencies.

### For Airflow 2.x (2.7+)

```bash
pip install 'acryl-datahub-airflow-plugin[airflow2]'
```

This installs the plugin with Legacy OpenLineage (`openlineage-airflow>=1.2.0`), which is required for Airflow 2.x lineage extraction.

#### Alternative: Using Native OpenLineage Provider on Airflow 2.7+

If your Airflow 2.7+ environment rejects the Legacy OpenLineage package (e.g., due to dependency conflicts), you can use the native OpenLineage provider instead:

```bash
# Install the native Airflow provider first
pip install 'apache-airflow-providers-openlineage>=1.0.0'

# Then install the DataHub plugin without OpenLineage extras
pip install acryl-datahub-airflow-plugin
```

The plugin will automatically detect and use `apache-airflow-providers-openlineage` when available, providing the same functionality.

### For Airflow 3.x (3.1+)

```bash
pip install 'acryl-datahub-airflow-plugin[airflow3]'
```

This installs the plugin with `apache-airflow-providers-openlineage>=1.0.0`, which is the native OpenLineage provider for Airflow 3.x.

**Note**: If using Airflow 3.0.x (3.0.6 specifically), you'll need to manually upgrade pydantic:

```bash
pip install 'acryl-datahub-airflow-plugin[airflow3]' 'pydantic>=2.11.8'
```

We recommend using Airflow 3.1.0+ which resolves this issue. See the Version Compatibility section above for details.

### What Gets Installed

#### Base Installation (No Extras)

When you install without any extras:

```bash
pip install acryl-datahub-airflow-plugin
```

You get:

- `acryl-datahub[sql-parser,datahub-rest]` - DataHub SDK with SQL parsing and REST emitter
- `pydantic>=2.4.0` - Required for data validation
- `apache-airflow>=2.5.0,<4.0.0` - Airflow itself
- **No OpenLineage package** - You'll need to provide your own or use one of the extras below

#### With `[airflow2]` Extra

```bash
pip install 'acryl-datahub-airflow-plugin[airflow2]'
```

Adds:

- `openlineage-airflow>=1.2.0` - Standalone OpenLineage package for Airflow 2.x

#### With `[airflow3]` Extra

```bash
pip install 'acryl-datahub-airflow-plugin[airflow3]'
```

Adds:

- `apache-airflow-providers-openlineage>=1.0.0` - Native OpenLineage provider for Airflow 3.x

### Additional Extras

You can combine multiple extras if needed:

```bash
# For Airflow 3.x with Kafka emitter support
pip install 'acryl-datahub-airflow-plugin[airflow3,datahub-kafka]'

# For Airflow 2.x with file emitter support
pip install 'acryl-datahub-airflow-plugin[airflow2,datahub-file]'
```

Available extras:

- `airflow2`: OpenLineage support for Airflow 2.x (adds `openlineage-airflow>=1.2.0`)
- `airflow3`: OpenLineage support for Airflow 3.x (adds `apache-airflow-providers-openlineage>=1.0.0`)
- `datahub-kafka`: Kafka-based metadata emission (adds `acryl-datahub[datahub-kafka]`)
- `datahub-file`: File-based metadata emission (adds `acryl-datahub[sync-file-emitter]`) - useful for testing

### Why Different Extras?

Airflow 2.x and 3.x have different OpenLineage integrations:

- **Airflow 2.x (2.5-2.6)** typically uses Legacy OpenLineage (`openlineage-airflow` package)
- **Airflow 2.x (2.7+)** can use either Legacy OpenLineage or native OpenLineage Provider (`apache-airflow-providers-openlineage`)
- **Airflow 3.x** uses native OpenLineage Provider (`apache-airflow-providers-openlineage`)

The plugin automatically detects which OpenLineage variant is installed and uses it accordingly. This means:

1. **With extras** (`[airflow2]` or `[airflow3]`): The appropriate OpenLineage dependency is installed automatically
2. **Without extras**: You provide your own OpenLineage installation, and the plugin auto-detects it

This flexibility allows you to adapt to different Airflow environments and dependency constraints.

## Configuration

The plugin can be configured via `airflow.cfg` under the `[datahub]` section. Below are the key configuration options:

### Extractor Patching (OpenLineage Enhancements)

When `enable_extractors=True` (default), the DataHub plugin enhances OpenLineage extractors to provide better lineage. You can fine-tune these enhancements:

```ini
[datahub]
# Enable/disable all OpenLineage extractors
enable_extractors = True  # Default: True

# Fine-grained control over DataHub's OpenLineage enhancements

# --- Patches (work with both Legacy OpenLineage and OpenLineage Provider) ---

# Patch SqlExtractor to use DataHub's advanced SQL parser (enables column-level lineage)
patch_sql_parser = True  # Default: True

# Patch SnowflakeExtractor to fix default schema detection
patch_snowflake_schema = True  # Default: True

# --- Custom Extractors (only apply to Legacy OpenLineage) ---

# Use DataHub's custom AthenaOperatorExtractor (better Athena lineage)
extract_athena_operator = True  # Default: True

# Use DataHub's custom BigQueryInsertJobOperatorExtractor (handles BQ job configuration)
extract_bigquery_insert_job_operator = True  # Default: True
```

**How it works:**

**Patches** (apply to both Legacy OpenLineage and OpenLineage Provider):

- Apply **monkey-patching** to OpenLineage extractor/operator classes at runtime
- Work on **both Airflow 2.x and Airflow 3.x**
- When `patch_sql_parser=True`:
  - **Airflow 2**: Patches `SqlExtractor.extract()` method
  - **Airflow 3**: Patches `SQLParser.generate_openlineage_metadata_from_sql()` method
  - Provides: More accurate lineage extraction, column-level lineage (CLL), better SQL dialect support
- When `patch_snowflake_schema=True`:
  - **Airflow 2**: Patches `SnowflakeExtractor.default_schema` property
  - **Airflow 3**: Currently not needed (handled by Airflow's native support)
  - Fixes Snowflake schema detection issues

**Custom Extractors/Operator Patches**:

- Register DataHub's custom implementations for specific operators
- Work on **both Airflow 2.x and Airflow 3.x**
- `extract_athena_operator`:
  - **Airflow 2 (Legacy OpenLineage only)**: Registers `AthenaOperatorExtractor`
  - **Airflow 3**: Patches `AthenaOperator.get_openlineage_facets_on_complete()`
  - Uses DataHub's SQL parser for better Athena lineage
- `extract_bigquery_insert_job_operator`:
  - **Airflow 2 (Legacy OpenLineage only)**: Registers `BigQueryInsertJobOperatorExtractor`
  - **Airflow 3**: Patches `BigQueryInsertJobOperator.get_openlineage_facets_on_complete()`
  - Handles BigQuery job configuration and destination tables

**Example use cases:**

Disable DataHub's SQL parser to use OpenLineage's native parsing:

```ini
[datahub]
enable_extractors = True
patch_sql_parser = False  # Use OpenLineage's native SQL parser
patch_snowflake_schema = True  # Still fix Snowflake schema detection
```

Disable custom Athena extractor (only relevant for Legacy OpenLineage):

```ini
[datahub]
enable_extractors = True
extract_athena_operator = False  # Use OpenLineage's default Athena extractor
```

### Other Configuration Options

For a complete list of configuration options, see the [DataHub Airflow documentation](https://docs.datahub.com/docs/lineage/airflow#configuration).

## Developing

See the [developing docs](../../metadata-ingestion/developing.md).
