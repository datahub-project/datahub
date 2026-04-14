"""
DataHub Airflow Plugin Example DAGs

This directory contains example DAGs demonstrating various features of the DataHub Airflow plugin.

## Directory Structure

- **airflow2/**: Example DAGs for Airflow 2.x with compatibility layers
- **airflow3/**: Example DAGs for Airflow 3.0+ using native syntax (no compatibility layers)
- Root directory: Legacy example DAGs with compatibility layers (deprecated, use airflow2/ or airflow3/)

## Choosing the Right Examples

- If you're using **Airflow 3.0+**, refer to examples in `airflow3/`
- If you're using **Airflow 2.x**, refer to examples in `airflow2/`
- For production DAGs that need to work across multiple Airflow versions, see `airflow2/` for compatibility patterns

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
- `graph_usage_sample_dag.py`: Complex DAG graph with multiple dependencies
"""
