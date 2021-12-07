# Using Airflow

If you are using Apache Airflow for your scheduling then you might want to use it for scheduling your ingestion receipes. To do that you can follow these steps
- Create a receipe file e.g. `recipe.yml`
- Ensure [DataHub CLI](../../docs/cli.md) is installed in your airflow environment
- Create a sample DAG file like [`generic_recipe_sample_dag.py`](../src/datahub_provider/example_dags/generic_recipe_sample_dag.py). This will read your DataHub ingestion recipe file and run it.
- Deploy the DAG file into airflow for scheduling.

Alternatively you can have an inline receipe as given in [`mysql_sample_dag.py`](../src/datahub_provider/example_dags/mysql_sample_dag.py). This runs a MySQL metadata ingestion pipeline using an inlined configuration.

You can go through [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/) for more details on how to use Airflow.