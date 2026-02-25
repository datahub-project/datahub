# Airflow Integration

:::note

If you're looking to schedule DataHub ingestion using Airflow, see the guide on [scheduling ingestion with Airflow](../../metadata-ingestion/schedule_docs/airflow.md).

:::

The DataHub Airflow plugin supports:

- Automatic column-level lineage extraction from various operators e.g. SQL operators (including `MySqlOperator`, `PostgresOperator`, `SnowflakeOperator`, `BigQueryInsertJobOperator`, and more), `S3FileTransformOperator`, and more.
- Airflow DAG and tasks, including properties, ownership, and tags.
- Task run information, including task successes and failures.
- Manual lineage annotations using `inlets` and `outlets` on Airflow operators.

The plugin requires Airflow 2.7+ and Python 3.10+. If you're using Airflow older than 2.7, it's possible to use the plugin with older versions of `acryl-datahub-airflow-plugin`. See the [compatibility section](#compatibility) for more details.

<!-- TODO: Update the local Airflow guide and link to it here. -->
<!-- If you are looking to run Airflow and DataHub using docker locally, follow the guide [here](../../docker/airflow/local_airflow.md). -->

## DataHub Plugin Setup

### Installation

The plugin requires Airflow 2.7+ and Python 3.10+. If you don't meet these requirements, see the [compatibility section](#compatibility) for other options.

```shell
pip install 'acryl-datahub-airflow-plugin>=1.1.0.4'
```

### Configuration

Set up a DataHub connection in Airflow, either via command line or the Airflow UI.

#### Command Line

```shell
airflow connections add  --conn-type 'datahub-rest' 'datahub_rest_default' --conn-host 'http://datahub-gms:8080' --conn-password '<optional datahub auth token>'
```

If you are using DataHub Cloud then please use `https://YOUR_PREFIX.acryl.io/gms` as the `--conn-host` parameter.

#### Airflow UI

On the Airflow UI, go to Admin -> Connections and click the "+" symbol to create a new connection. Select "DataHub REST Server" from the dropdown for "Connection Type" and enter the appropriate values.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/airflow/plugin_connection_setup.png"/>
</p>

#### Optional Configurations

No additional configuration is required to use the plugin. However, there are some optional configuration parameters that can be set in the `airflow.cfg` file.

```ini title="airflow.cfg"
[datahub]
# Optional - additional config here.
enabled = True  # default
```

| Name                               | Default value        | Description                                                                                                                      |
| ---------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| enabled                            | true                 | If the plugin should be enabled.                                                                                                 |
| conn_id                            | datahub_rest_default | The name of the datahub rest connection.                                                                                         |
| cluster                            | prod                 | name of the airflow cluster, this is equivalent to the `env` of the instance                                                     |
| platform_instance                  | None                 | The instance of the platform that all assets produced by this plugin belong to. It is optional.                                  |
| capture_ownership_info             | true                 | Extract DAG ownership.                                                                                                           |
| capture_ownership_as_group         | false                | When extracting DAG ownership, treat DAG owner as a group rather than a user                                                     |
| capture_tags_info                  | true                 | Extract DAG tags.                                                                                                                |
| capture_executions                 | true                 | Extract task runs and success/failure statuses. This will show up in DataHub "Runs" tab.                                         |
| materialize_iolets                 | true                 | Create or un-soft-delete all entities referenced in lineage.                                                                     |
| enable_extractors                  | true                 | Enable automatic lineage extraction.                                                                                             |
| disable_openlineage_plugin         | true                 | Disable the OpenLineage plugin to avoid duplicative processing.                                                                  |
| enable_multi_statement_sql_parsing | false                | Parse multiple SQL statements within a single task. Resolves temp tables and merges lineage across statements in one execution.  |
| log_level                          | _no change_          | [debug] Set the log level for the plugin.                                                                                        |
| debug_emitter                      | false                | [debug] If true, the plugin will log the emitted events.                                                                         |
| dag_filter_str                     | { "allow": [".*"] }  | AllowDenyPattern value in form of JSON string to filter the DAGs from running.                                                   |
| enable_datajob_lineage             | true                 | If true, the plugin will emit input/output lineage for DataJobs.                                                                 |
| capture_airflow_assets             | true                 | Capture native Airflow Assets/Datasets as DataHub lineage. See [Native Airflow Assets/Datasets](#native-airflow-assetsdatasets). |

## Automatic lineage extraction

To automatically extract lineage information, the plugin builds on top of Airflow's built-in [OpenLineage extractors](https://openlineage.io/docs/integrations/airflow/default-extractors).
As such, we support a superset of the default operators that Airflow/OpenLineage supports.

The SQL-related extractors have been updated to use [DataHub's SQL lineage parser](./sql_parsing.md), which is more robust than the built-in one and uses DataHub's metadata information to generate column-level lineage.

Supported operators:

- `SQLExecuteQueryOperator`, including any subclasses. Note that in newer versions of Airflow (generally Airflow 2.5+), most SQL operators inherit from this class.
- `AthenaOperator` and `AWSAthenaOperator`
- `BigQueryOperator` and `BigQueryExecuteQueryOperator`
- `BigQueryInsertJobOperator` (incubating)
- `MySqlOperator`
- `PostgresOperator`
- `RedshiftSQLOperator`
- `SnowflakeOperator` and `SnowflakeOperatorAsync`
- `SqliteOperator`
- `TeradataOperator` (_Note: Teradata uses two-tier `database.table` naming without a schema level_)
- `TrinoOperator`

<!--
These operators are supported by OpenLineage, but we haven't tested them yet:
- `SQLCheckOperator`, `SQLValueCheckOperator`, `SQLThresholdCheckOperator`, `SQLIntervalCheckOperator`, `SQLColumnCheckOperator`, `BigQueryColumnCheckOperator`
- `FTPFileTransmitOperator`
- `GCSToGCSOperator`
- `S3CopyObjectOperator`
- `S3FileTransformOperator`
- `SageMakerProcessingOperator` and `SageMakerProcessingOperatorAsync`
- `SFTPOperator`

There's also a few operators (e.g. BashOperator, PythonOperator) that have custom extractors, but those extractors don't generate lineage.
-->

### Multi-Statement SQL Parsing

When a task executes multiple SQL statements (e.g., `CREATE TEMP TABLE ...; INSERT ... FROM temp_table;`), enable this to parse all statements together and resolve temporary table dependencies. By default (False), only the first statement is parsed.

```ini title="airflow.cfg"
[datahub]
enable_multi_statement_sql_parsing = True  # Default: False
```

**Note:** Use a list of SQL strings (recommended) or semicolon-separated statements in a single string:

## Manual Lineage Annotation

### Using `inlets` and `outlets`

You can manually annotate lineage by setting `inlets` and `outlets` on your Airflow operators. This is useful if you're using an operator that doesn't support automatic lineage extraction, or if you want to override the automatic lineage extraction.

We have a few code samples that demonstrate how to use `inlets` and `outlets`:

- [`lineage_backend_demo.py`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/lineage_backend_demo.py)
- [`lineage_backend_taskflow_demo.py`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/lineage_backend_taskflow_demo.py) - uses the [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html)

For more information, take a look at the [Airflow lineage docs](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html).

### Native Airflow Assets/Datasets

Starting with Airflow 2.4+, you can use native Airflow [Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html) (renamed to [Assets](https://airflow.apache.org/docs/apache-airflow/3.0.0/authoring-and-scheduling/assets.html) in Airflow 3.x) for data-aware scheduling. The DataHub plugin automatically captures these as lineage when used in `inlets` and `outlets`.

```python
from airflow.sdk.definitions.asset import Asset  # Airflow 3.x
# or: from airflow.datasets import Dataset as Asset  # Airflow 2.4+

s3_input = Asset("s3://my-bucket/input/data.parquet")
bigquery_output = Asset("bigquery://my-project/dataset/result_table")

task = BashOperator(
    task_id="process_data",
    bash_command="echo 'Processing'",
    inlets=[s3_input],
    outlets=[bigquery_output],
)
```

The plugin maps URI schemes to DataHub platforms:

| URI Scheme            | DataHub Platform |
| --------------------- | ---------------- |
| `s3://`, `s3a://`     | s3               |
| `gs://`, `gcs://`     | gcs              |
| `postgresql://`       | postgres         |
| `mysql://`            | mysql            |
| `bigquery://`         | bigquery         |
| `snowflake://`        | snowflake        |
| `file://`             | file             |
| `hdfs://`             | hdfs             |
| `abfs://`, `abfss://` | adls             |

Plain name assets (e.g., from the `@asset` decorator) default to the `airflow` platform.

#### Configuration

```ini title="airflow.cfg"
[datahub]
# Set to false to disable capturing Airflow Assets as lineage (default: true)
capture_airflow_assets = true
```

#### Limitations

Native Airflow Assets have the following limitations compared to using DataHub's `Dataset` or `Urn` entities directly:

1. **No `platform_instance` support**: The URN generated from an Airflow Asset URI cannot include a platform instance. The plugin only extracts the platform, dataset name, and environment from the URI.

2. **Environment uses global plugin config**: All native Airflow Assets use the `cluster` setting from the plugin configuration as their environment. You cannot specify a different environment per asset.

If you need `platform_instance` or per-asset environment control, use the DataHub entity classes instead:

```python
from datahub_airflow_plugin.entities import Dataset

# Full control over URN components
s3_input = Dataset(
    platform="s3",
    name="my-bucket/input/data.parquet",
    env="PROD",
    platform_instance="us-west-2"  # Specify platform instance
)

task = BashOperator(
    task_id="process_data",
    bash_command="echo 'Processing'",
    inlets=[s3_input],
)
```

### Custom Operators

If you have created a [custom Airflow operator](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html) that inherits from the BaseOperator class,
when overriding the `execute` function, set inlets and outlets via `context['ti'].task.inlets` and `context['ti'].task.outlets`.
The DataHub Airflow plugin will then pick up those inlets and outlets after the task runs.

You can only set table-level lineage using inlets and outlets. For column-level lineage, you need to write a custom extractor for your custom operator.

```python
class DbtOperator(BaseOperator):
    ...

    def execute(self, context):
        # do something
        inlets, outlets = self._get_lineage()
        # inlets/outlets are lists of either datahub_airflow_plugin.entities.Dataset or datahub_airflow_plugin.entities.Urn
        context['ti'].task.inlets = self.inlets
        context['ti'].task.outlets = self.outlets

    def _get_lineage(self):
        # Do some processing to get inlets/outlets

        return inlets, outlets
```

If you override the `pre_execute` and `post_execute` function, ensure they include the `@prepare_lineage` and `@apply_lineage` decorators respectively. Reference the [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/lineage.html#lineage) for more details.

See example implementation of a custom operator using SQL parser to capture table level lineage [here](../../metadata-ingestion-modules/airflow-plugin/tests/integration/dags/airflow3/custom_operator_sql_parsing.py)

### Custom SQL Operators with Automatic Lineage

If you're building a custom SQL operator, you have two approaches depending on your needs:

#### Option 1: Inherit from SQLExecuteQueryOperator (Recommended)

**This is the easiest approach** - inherit from Airflow's `SQLExecuteQueryOperator` and you automatically get:

- ✅ OpenLineage support built-in
- ✅ DataHub's enhanced SQL parser (via our SQLParser patch)
- ✅ Column-level lineage extraction
- ✅ No extra code needed

```python
from typing import Any
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class MyCustomSQLOperator(SQLExecuteQueryOperator):
    """
    Custom SQL operator that inherits OpenLineage support.

    DataHub automatically enhances the SQL parsing with column-level lineage!
    """

    def __init__(self, my_custom_param: str, **kwargs):
        # Add your custom parameters
        self.my_custom_param = my_custom_param
        super().__init__(**kwargs)

    def execute(self, context: Any) -> Any:
        # Add any custom logic before SQL execution
        self.log.info(f"Custom param: {self.my_custom_param}")

        # Parent class handles SQL execution + OpenLineage lineage
        return super().execute(context)
```

**How it works:**

1. `SQLExecuteQueryOperator` already has `get_openlineage_facets_on_complete()` implemented
2. It uses the hook's OpenLineage methods, which internally call `SQLParser`
3. DataHub patches `SQLParser` globally, so all SQL parsing gets enhanced automatically
4. You get column-level lineage without writing any lineage-specific code!

**When to use this:**

- Building operators for standard SQL databases (Postgres, MySQL, Snowflake, BigQuery, etc.)
- Want the simplest integration
- Don't need custom lineage extraction logic

#### Option 2: Implement OpenLineage Interface from Scratch

Only needed if you're building a completely custom operator that doesn't fit the `SQLExecuteQueryOperator` pattern:

```python
from typing import Any, Optional
from airflow.models.baseoperator import BaseOperator

class MyCompletelyCustomOperator(BaseOperator):
    """
    For special cases where SQLExecuteQueryOperator doesn't fit.
    """

    def execute(self, context: Any) -> Any:
        # Your custom SQL execution logic
        pass

    def get_openlineage_facets_on_complete(
        self, task_instance: Any
    ) -> Optional["OperatorLineage"]:
        """
        Implement OpenLineage interface manually.
        DataHub's SQLParser patch still enhances this automatically!
        """
        from airflow.providers.openlineage.sqlparser import SQLParser

        hook = self.get_db_hook()
        parser = SQLParser(
            dialect=hook.get_openlineage_database_dialect(hook.get_connection(self.conn_id)),
            default_schema=hook.get_openlineage_default_schema(),
        )

        # This uses DataHub's patched SQLParser - column lineage included!
        return parser.generate_openlineage_metadata_from_sql(
            sql=self.sql,
            hook=hook,
            database_info=hook.get_openlineage_database_info(hook.get_connection(self.conn_id)),
        )
```

**When to use this:**

- Building a very specialized operator from scratch
- Need complete control over lineage extraction
- The standard SQLExecuteQueryOperator pattern doesn't apply

**Key Point:** DataHub patches `SQLParser.generate_openlineage_metadata_from_sql()` globally at import time, so **any operator** using OpenLineage's SQLParser automatically gets DataHub's enhanced parsing with column-level lineage!

### Alternative: Custom Operators with Manual Lineage (Airflow 2.x and 3.x)

If you prefer not to use OpenLineage, or are on older Airflow versions, you can manually extract and set lineage using DataHub's SQL parser:

```python
from typing import Any, List, Tuple, Union
from airflow.models.baseoperator import BaseOperator
from datahub_airflow_plugin._config import get_enable_multi_statement
from datahub_airflow_plugin._sql_parsing_common import parse_sql_with_datahub
from datahub_airflow_plugin.entities import Urn

class CustomSQLOperator(BaseOperator):
    def __init__(self, sql: Union[str, List[str]], database: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.sql = sql
        self.database = database

    def execute(self, context: Any) -> Any:
        # Execute SQL
        # ...

        # Extract and set lineage
        inlets, outlets = self._get_lineage()
        context["ti"].task.inlets = inlets
        context["ti"].task.outlets = outlets

    def _get_lineage(self) -> Tuple[List, List]:
        # Get multi-statement config flag
        enable_multi_statement = get_enable_multi_statement()

        # Parse SQL with multi-statement support
        # Handles both string and list of SQL statements
        sql_parsing_result = parse_sql_with_datahub(
            sql=self.sql,
            platform="postgres",  # your platform
            default_database=self.database,
            env="PROD",
            default_schema=None,
            graph=None,
            enable_multi_statement=enable_multi_statement,
        )

        inlets = [Urn(table) for table in sql_parsing_result.in_tables]
        outlets = [Urn(table) for table in sql_parsing_result.out_tables]
        return inlets, outlets
```

See [full example](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/airflow-plugin/tests/integration/dags/airflow3/custom_operator_sql_parsing.py).

### Custom Extractors (Advanced - Legacy OpenLineage)

For advanced use cases with the legacy OpenLineage package (`openlineage-airflow`), you can create a custom extractor. This is useful if you're using a built-in Airflow operator for which we don't support automatic lineage extraction.

See this [example PR](https://github.com/datahub-project/datahub/pull/10452) which adds a custom extractor for the `BigQueryInsertJobOperator` operator.

## Cleanup obsolete pipelines and tasks from Datahub

There might be a case where the DAGs are removed from the Airflow but the corresponding pipelines and tasks are still there in the Datahub, let's call such pipelines ans tasks, `obsolete pipelines and tasks`

Following are the steps to cleanup them from the datahub:

- create a DAG named `Datahub_Cleanup`, i.e.

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from datahub_airflow_plugin.entities import Dataset, Urn

with DAG(
    "Datahub_Cleanup",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = BashOperator(
        task_id="cleanup_obsolete_data",
        dag=dag,
        bash_command="echo 'cleaning up the obsolete data from datahub'",
    )

```

- ingest this DAG, and it will remove all the obsolete pipelines and tasks from the Datahub based on the `cluster` value set in the `airflow.cfg`

## Get all dataJobs associated with a dataFlow

If you are looking to find all tasks (aka DataJobs) that belong to a specific pipeline (aka DataFlow), you can use the following GraphQL query:

```graphql
query {
  dataFlow(urn: "urn:li:dataFlow:(airflow,db_etl,prod)") {
    childJobs: relationships(
      input: { types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100 }
    ) {
      total
      relationships {
        entity {
          ... on DataJob {
            urn
          }
        }
      }
    }
  }
}
```

## Emit Lineage Directly

If you can't use the plugin or annotate inlets/outlets, you can also emit lineage using the `DatahubEmitterOperator`.

Reference [`lineage_emission_dag.py`](../../metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/example_dags/lineage_emission_dag.py) for a full example.

In order to use this example, you must first configure the Datahub hook. Like in ingestion, we support a Datahub REST hook and a Kafka-based hook. See the plugin configuration for examples.

## Debugging

### Missing lineage

If you're not seeing lineage in DataHub, check the following:

- Validate that the plugin is loaded in Airflow. Go to Admin -> Plugins and check that the DataHub plugin is listed.
- It should also print a log line like `INFO  [datahub_airflow_plugin.datahub_listener] DataHub plugin using DataHubRestEmitter: configured to talk to <datahub_url>` during Airflow startup, and the `airflow plugins` command should list `datahub_plugin` with a listener enabled.
- If using the plugin's automatic lineage, ensure that the `enable_extractors` config is set to true and that automatic lineage is supported for your operator.
- If using manual lineage annotation, ensure that you're using the `datahub_airflow_plugin.entities.Dataset` or `datahub_airflow_plugin.entities.Urn` classes for your inlets and outlets.

### Incorrect URLs

If your URLs aren't being generated correctly (usually they'll start with `http://localhost:8080` instead of the correct hostname), you may need to set the webserver `base_url` config.

```ini title="airflow.cfg"
[webserver]
base_url = http://airflow.mycorp.example.com
```

### TypeError ... missing 3 required positional arguments

If you see errors like the following:

```shell
ERROR - on_task_instance_success() missing 3 required positional arguments: 'previous_state', 'task_instance', and 'session'
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.8/site-packages/datahub_airflow_plugin/datahub_listener.py", line 124, in wrapper
    f(*args, **kwargs)
TypeError: on_task_instance_success() missing 3 required positional arguments: 'previous_state', 'task_instance', and 'session'
```

The solution is to upgrade `acryl-datahub-airflow-plugin>=0.12.0.4` or upgrade `pluggy>=1.2.0`. See this [PR](https://github.com/datahub-project/datahub/pull/9365) for details.

### Scheduler stalling

For extremely large Airflow deployments with thousands of tasks, you may see issues where the plugin interferes with the performance of the Airflow scheduler. In those cases, you can set the `DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT=0` environment variable. This makes the DataHub plugin run fully in background threads, but can cause us to miss some metadata if the scheduler shuts down soon after processing a task.

### Disabling the DataHub Plugin

There are two ways to disable the DataHub Plugin:

#### 1. Disable via Configuration

Set the `datahub.enabled` configuration property to `False` in the `airflow.cfg` file and restart the Airflow environment to reload the configuration and disable the plugin.

```ini title="airflow.cfg"
[datahub]
enabled = False
```

#### 2. Disable via Environment Variable (Kill-Switch)

If a restart is not possible and you need a faster way to disable the plugin, you can use the kill-switch. Set the `AIRFLOW_VAR_DATAHUB_AIRFLOW_PLUGIN_DISABLE_LISTENER` environment variable to `true`. This ensures that the listener won't process anything.

```shell
export AIRFLOW_VAR_DATAHUB_AIRFLOW_PLUGIN_DISABLE_LISTENER=true
```

This will immediately disable the plugin without requiring a restart.

:::note Why Environment Variable Instead of Airflow Variable?
The plugin uses environment variables instead of Airflow's `Variable.get()` because listener hooks are called during SQLAlchemy's `after_flush` event (before the main transaction commits). Calling `Variable.get()` in this context creates a nested database session that can interfere with the outer transaction and cause data loss, such as missing TaskInstanceHistory records for retried tasks.
:::

## Compatibility

We try to support Airflow releases for ~2 years after their release. This is a best-effort guarantee - it's not always possible due to dependency / security issues cropping up in older versions.

We no longer officially support Airflow <2.7. However, you can use older versions of `acryl-datahub-airflow-plugin` with older versions of Airflow.
We previously had two implementations of the plugin - v1 and v2. The v2 plugin is now the default, and the v1 plugin has since been removed. The v1 plugin had many limitations, chiefly that it does not support automatic lineage extraction. Docs for the v1 plugin can be accessed in our [docs archive](https://docs-website-r5eolot5n-acryldata.vercel.app/docs/lineage/airflow#datahub-plugin-v1).

All recent versions require Python 3.10+.

- Airflow 1.10.x, use acryl-datahub-airflow-plugin <= 0.9.1.0 (v1 plugin).
- Airflow 2.0.x, use acryl-datahub-airflow-plugin <= 0.11.0.1 (v1 plugin).
- Airflow 2.2.x, use acryl-datahub-airflow-plugin <= 0.14.1.5 (v2 plugin).
- Airflow 2.3 - 2.4.3, use acryl-datahub-airflow-plugin <= 1.0.0 (v2 plugin).
- Airflow 2.5 and 2.6, use acryl-datahub-airflow-plugin <= 1.1.0.4 (v2 plugin).

DataHub also previously supported an Airflow [lineage backend](https://airflow.apache.org/docs/apache-airflow/2.2.0/lineage.html#lineage-backend) implementation. The lineage backend functionality was pretty limited - it did not support automatic lineage extraction, did not capture task failures, and did not work in AWS MWAA - and so it has been removed from the codebase. The [documentation for the lineage backend](https://docs-website-1wmaehubl-acryldata.vercel.app/docs/lineage/airflow/#using-datahubs-airflow-lineage-backend-deprecated) has been archived.

## Additional references

Related Datahub videos:

- [Airflow Lineage](https://www.youtube.com/watch?v=3wiaqhb8UR0)
- [Airflow Run History in DataHub](https://www.youtube.com/watch?v=YpUOqDU5ZYg)
