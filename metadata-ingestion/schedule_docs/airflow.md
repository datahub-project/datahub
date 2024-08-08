# Using Airflow

If you are using Apache Airflow for your scheduling then you might want to also use it for scheduling your ingestion recipes. For any Airflow specific questions you can go through [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/) for more details.

We've provided a few examples of how to configure your DAG:

- [`mysql_sample_dag`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/mysql_sample_dag.py) embeds the full MySQL ingestion configuration inside the DAG.

- [`snowflake_sample_dag`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/snowflake_sample_dag.py) avoids embedding credentials inside the recipe, and instead fetches them from Airflow's [Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection/index.html) feature. You must configure your connections in Airflow to use this approach.

:::tip

These example DAGs use the `PythonVirtualenvOperator` to run the ingestion. This is the recommended approach, since it guarantees that there will not be any conflicts between DataHub and the rest of your Airflow environment.

When configuring the task, it's important to specify the requirements with your source and set the `system_site_packages` option to false.

```py
ingestion_task = PythonVirtualenvOperator(
	task_id="ingestion_task",
	requirements=[
		"acryl-datahub[<your-source>]",
	],
	system_site_packages=False,
	python_callable=your_callable,
)
```

:::

<details>
<summary>Advanced: loading a recipe file</summary>

In more advanced cases, you might want to store your ingestion recipe in a file and load it from your task.

- Ensure the recipe file is in a folder accessible to your airflow workers. You can either specify absolute path on the machines where Airflow is installed or a path relative to `AIRFLOW_HOME`.
- Ensure [DataHub CLI](../../docs/cli.md) is installed in your airflow environment.
- Create a DAG task to read your DataHub ingestion recipe file and run it. See the example below for reference.
- Deploy the DAG file into airflow for scheduling. Typically this involves checking in the DAG file into your dags folder which is accessible to your Airflow instance.

Example: [`generic_recipe_sample_dag`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/generic_recipe_sample_dag.py)

</details>
