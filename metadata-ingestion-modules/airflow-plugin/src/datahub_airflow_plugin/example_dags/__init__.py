"""
DataHub Airflow Plugin Example DAGs

Example DAGs demonstrating various features of the DataHub Airflow plugin.

## Available Examples

### Lineage Collection
- `lineage_backend_demo.py`: Basic lineage collection using inlets/outlets
- `lineage_backend_taskflow_demo.py`: Lineage collection with TaskFlow API

### Data Ingestion
- `snowflake_sample_dag.py`: Ingest Snowflake metadata into DataHub
- `mysql_sample_dag.py`: Ingest MySQL metadata into DataHub
- `generic_recipe_sample_dag.py`: Run any DataHub recipe from Airflow

### Advanced Features
- `lineage_emission_dag.py`: Custom lineage emission with DatahubEmitterOperator
- `graph_usage_sample_dag.py`: Use the DataHubGraph client from within a DAG
"""
