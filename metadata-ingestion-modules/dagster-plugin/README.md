# Datahub Dagster Plugin

See the [DataHub Dagster docs](https://docs.datahub.com/docs/lineage/dagster/) for details.

## Sibling links to warehouse tables

Set `emit_siblings=True` in `DatahubDagsterSourceConfig` to link each Dagster asset to the
warehouse table it materializes via a `siblings` relationship. The Dagster asset and the
warehouse dataset then render as a single merged entity in DataHub. Use
`dagster_is_primary_sibling` to choose which side is primary (defaults to the warehouse
table). See `examples/basic_setup.py`.
