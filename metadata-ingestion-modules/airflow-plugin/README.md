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

This installs the plugin with `openlineage-airflow>=1.2.0`, which is required for Airflow 2.x lineage extraction.

**Backward compatibility**: `plugin-v2` is still supported as an alias for `airflow2`.

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

### Additional Extras

You can combine multiple extras if needed:

```bash
# For Airflow 3.x with Kafka emitter support
pip install 'acryl-datahub-airflow-plugin[airflow3,datahub-kafka]'

# For Airflow 2.x with file emitter support
pip install 'acryl-datahub-airflow-plugin[airflow2,datahub-file]'
```

Available extras:

- `airflow2`: OpenLineage support for Airflow 2.x (preferred)
- `airflow3`: OpenLineage support for Airflow 3.x (preferred)
- `plugin-v2`: Alias for `airflow2` (backward compatibility)
- `datahub-kafka`: Kafka-based metadata emission
- `datahub-file`: File-based metadata emission (useful for testing)

### Why Different Extras?

Airflow 2.x and 3.x have different OpenLineage integrations:

- **Airflow 2.x** uses the standalone `openlineage-airflow` package
- **Airflow 3.x** has native OpenLineage support via `apache-airflow-providers-openlineage`

The plugin automatically detects your Airflow version and uses the appropriate integration.

## Developing

See the [developing docs](../../metadata-ingestion/developing.md).
