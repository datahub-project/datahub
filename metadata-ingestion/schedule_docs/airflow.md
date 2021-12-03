# Scheduling Ingestion using Airflow

Take a look at these sample DAGs:

- [`generic_recipe_sample_dag.py`](../src/datahub_provider/example_dags/generic_recipe_sample_dag.py) - reads a DataHub ingestion recipe file and runs it
- [`mysql_sample_dag.py`](../src/datahub_provider/example_dags/mysql_sample_dag.py) - runs a MySQL metadata ingestion pipeline using an inlined configuration.