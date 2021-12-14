# Using Airflow

If you are using Apache Airflow for your scheduling then you might want to also use it for scheduling your ingestion recipes. For any Airflow specific questions you can go through [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/) for more details.

To schedule your recipe through Airflow you can follow these steps
- Create a recipe file e.g. `recipe.yml`
- Ensure the receipe file is in a folder accessible to your airflow workers. You can either specify absolute path on the machines where Airflow is installed or a path relative to `AIRFLOW_HOME`.
- Ensure [DataHub CLI](../../docs/cli.md) is installed in your airflow environment
- Create a sample DAG file like [`generic_recipe_sample_dag.py`](../src/datahub_provider/example_dags/generic_recipe_sample_dag.py). This will read your DataHub ingestion recipe file and run it.
- Deploy the DAG file into airflow for scheduling. Typically this involves checking in the DAG file into your dags folder which is accessible to your Airflow instance.

Alternatively you can have an inline recipe as given in [`mysql_sample_dag.py`](../src/datahub_provider/example_dags/mysql_sample_dag.py). This runs a MySQL metadata ingestion pipeline using an inlined configuration.
