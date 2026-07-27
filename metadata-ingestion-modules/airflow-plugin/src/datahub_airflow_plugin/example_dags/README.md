# DataHub Airflow Plugin Example DAGs

Example DAGs demonstrating various features of the DataHub Airflow plugin. All
examples target Airflow 3.1+.

## Example Categories

### 1. Lineage Collection

Examples showing how to automatically collect lineage from your Airflow DAGs:

- **lineage_backend_demo.py**: Basic lineage collection using `inlets` and `outlets`
- **lineage_backend_taskflow_demo.py**: Lineage with TaskFlow API (`@task` decorator)

### 2. Metadata Ingestion

Examples showing how to ingest metadata from data sources into DataHub:

- **snowflake_sample_dag.py**: Ingest Snowflake metadata
- **mysql_sample_dag.py**: Ingest MySQL metadata
- **generic_recipe_sample_dag.py**: Run any DataHub ingestion recipe

### 3. Advanced Features

- **lineage_emission_dag.py**: Directly emit custom lineage using `DatahubEmitterOperator`
- **graph_usage_sample_dag.py**: Use the `DataHubGraph` client from within a DAG

## Running the Examples

1. Copy the example to your Airflow DAGs folder:

   ```bash
   cp lineage_backend_demo.py $AIRFLOW_HOME/dags/
   ```

2. Configure the DataHub connection in Airflow:

   ```bash
   airflow connections add datahub_rest_default \
     --conn-type datahub-rest \
     --conn-host http://localhost:8080
   ```

3. Enable the DAG in the Airflow UI or trigger it manually:
   ```bash
   airflow dags trigger datahub_lineage_backend_demo
   ```

## Additional Resources

- [DataHub Airflow Plugin Documentation](https://datahubproject.io/docs/lineage/airflow/)
- [DataHub Metadata Ingestion](https://datahubproject.io/docs/metadata-ingestion/)
