# Datahub Airflow Plugin

See [the DataHub Airflow docs](https://docs.datahub.com/docs/lineage/airflow) for details.

## Version Compatibility

The plugin supports Apache Airflow versions 2.7+ and 3.1+.

**Note on Airflow 3.0.x**: Airflow 3.0.6 pins pydantic==2.11.7, which contains a bug that prevents the DataHub plugin from importing correctly. This issue is resolved in Airflow 3.1.0+ which uses pydantic>=2.11.8. If you must use Airflow 3.0.6, you can manually upgrade pydantic to >=2.11.8, though this may conflict with Airflow's dependency constraints. We recommend upgrading to Airflow 3.1.0 or later.

Related issue: https://github.com/pydantic/pydantic/issues/10963

## Developing

See the [developing docs](../../metadata-ingestion/developing.md).
