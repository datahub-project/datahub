# Datahub Airflow Plugin

## Capabilities

DataHub supports integration of

- Airflow Pipeline (DAG) metadata
- DAG and Task run information
- Lineage information when present

## Installation

1. You need to install the required dependency in your airflow.

  ```shell
    pip install acryl-datahub-airflow-plugin
  ```

::: note

We recommend you use the lineage plugin if you are on Airflow version >= 2.0.2 or on MWAA with an Airflow version >= 2.0.2
:::

2. Disable lazy plugin load in your airflow.cfg

  ```yaml
  core.lazy_load_plugins : False
  ```

3. You must configure an Airflow hook for Datahub. We support both a Datahub REST hook and a Kafka-based hook, but you only need one.

   ```shell
   # For REST-based:
   airflow connections add  --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://localhost:8080'
   # For Kafka-based (standard Kafka sink config can be passed via extras):
   airflow connections add  --conn-type 'datahub_kafka' 'datahub_kafka_default' --conn-host 'broker:9092' --conn-extra '{}'
   ```

4. Add your `datahub_conn_id` and/or `cluster` to your `airflow.cfg` file if it is not align with the default values. See configuration parameters below

    **Configuration options:**

    |Name   | Default value   | Description   |
    |---|---|---|
    | datahub.datahub_conn_id | datahub_rest_deafault  | The name of the datahub connection you set in step 1.  |
    | datahub.cluster |  prod | name of the airflow cluster  |
    | datahub.capture_ownership_info | true  |  If true, the owners field of the DAG will be capture as a DataHub corpuser.   |
    | datahub.capture_tags_info  | true   | If true, the tags field of the DAG will be captured as DataHub tags.  |
    | datahub.graceful_exceptions  | true  | If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall task to fail. Note that configuration issues will still throw exceptions.|

5. Configure `inlets` and `outlets` for your Airflow operators. For reference, look at the sample DAG in [`lineage_backend_demo.py`](../../metadata-ingestion/src/datahub_provider/example_dags/lineage_backend_demo.py), or reference [`lineage_backend_taskflow_demo.py`](../../metadata-ingestion/src/datahub_provider/example_dags/lineage_backend_taskflow_demo.py) if you're using the [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html).
6. [optional] Learn more about [Airflow lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html), including shorthand notation and some automation.

## How to validate installation

  1. Go and check in Airflow at Admin -> Plugins menu if you can see the Datahub plugin
  2. Run an Airflow DAG and you should see in the task logs Datahub releated log messages like:

  ```
  Emitting Datahub ...
  ```

## Additional references

Related Datahub videos:
[Airflow Lineage](https://www.youtube.com/watch?v=3wiaqhb8UR0)
[Airflow Run History in DataHub](https://www.youtube.com/watch?v=YpUOqDU5ZYg)
