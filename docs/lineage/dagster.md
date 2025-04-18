# Dagster Integration

This connector supports extracting:

- Dagster Pipeline and Task Metadata
- Pipeline Run Status
- Table Lineage

from Dagster.

## Supported Versions

This integration was verified using Dagster 1.7.0+. That does not necessary mean it will not be compatible will older versions. 

## Using DataHub's Dagster Sensor

Dagster Sensors allow us to perform actions when important events occur in Dagster. DataHub's Dagster Sensor allows you to emit metadata after every Dagster pipeline run. This sensor can emit Pipelines, Tasks, and run results. For more details about Dagster Sensors, please refer to [the documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors).

### Prerequisites

1. Create a Dagster project. See [Create New Project](https://docs.dagster.io/getting-started/create-new-project).
2. Create a [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions) class or [Repositories](https://docs.dagster.io/concepts/repositories-workspaces/repositories#repositories).
3. The creation of a new Dagster project via the UI uses the Definition class to define Dagster pipelines.

### Setup

1. Install the DataHub Dagster plugin package:

    ```shell
    pip install acryl_datahub_dagster_plugin
    ```

2. Import the DataHub Dagster Sensor, which is provided in the plugin package, to your Dagster Definition or Repository before starting the Dagster UI:

    **Example Definitions Class:**

    ```python
    {{ inline /metadata-ingestion-modules/dagster-plugin/examples/basic_setup.py }}
    ```

3. The DataHub Dagster plugin-provided sensor internally uses the following configurations. You can override the default config values using environment variables.

   **Configuration options:**

   | Configuration Option          | Default Value | Description                                                                                                                                                                                                                                                                                                                       |
   |-------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | datahub_client_config         |               | The DataHub client config                                                                                                                                                                                                                                                                                                         |
   | dagster_url                   |               | The URL to your Dagster Webserver.                                                                                                                                                                                                                                                                                                |
   | capture_asset_materialization | True          | Whether to capture asset keys as DataHub Datasets on AssetMaterialization event                                                                                                                                                                                                                                                            |
   | capture_input_output          | True          | Whether to capture and try to parse input and output from HANDLED_OUTPUT, LOADED_INPUT events. (currently only [PathMetadataValue](https://github.com/dagster-io/dagster/blob/7e08c05dcecef9fd07f887c7846bd1c9a90e7d84/python_modules/dagster/dagster/_core/definitions/metadata/__init__.py#L655) metadata supported (EXPERIMENTAL) |
   | platform_instance             |               | The instance of the platform that all assets produced by this recipe belong to. It is optional                                                                                                                                                                                                                                    |
   | asset_lineage_extractor       |               | You can implement your own logic to capture asset lineage information. See example for details[]                                                                                                                                                                                                                                  |
   | enable_asset_query_metadata_parsing |           | Whether to enable parsing query from asset metadata. See below for details[]                                                                                                                                                                                                                                  |

4. Once the Dagster UI is running, turn on the provided Sensor execution. To turn on the Sensor, click on the **Overview** tab and then on the **Sensors** tab. Simply toggle the DataHub sensor on.

Woohoo! Now, the DataHub Sensor is ready to emit metadata after every pipeline run.

### How to Validate Installation

1. Navigate to the Dagster UI.
2. Go to **Overview** > **Sensors** and look for `datahub_sensor`.
3. Start a Dagster Job. In the daemon logs, you should see DataHub-related log messages:

    ```
    datahub_sensor - Emitting metadata...
    ```

    This means that DataHub's sensor is correctly configured to emit metadata to DataHub.

## Capturing Table Lineage

There are a few ways to extract lineage, or relationships between tables, from Dagster. We recommend one or more of the following approaches to extract lineage automatically.

### Extracting Lineage from SQL Queries

#### But First: Extracting Asset Identifiers

When naming Dagster Assets, we recommend the following structure:

`key_prefix=["env", "platform", "db_name", "schema_name"]`

This ensures that we correctly resolve the Asset name to a Dataset URN in DataHub.

For example:

```python
@asset(
    key_prefix=["prod", "snowflake", "db_name", "schema_name"], # the fqdn asset name to be able to identify platform and make sure the asset is unique
    deps=[iris_dataset],
)
```

If you properly name your Dagster Asset, you can establish a connection between the Asset and the dataset it is referring to, which is likely already stored in DataHub. This allows for accurate tracking and lineage information in the next steps.

If you follow a different naming convention, you can create your own `asset_keys_to_dataset_urn_converter` logic and set a custom callback function. This can be used to generate a DataHub Dataset URN in any way you please, from metadata or otherwise.

Here is an example that can create a DataHub URN from the Asset key naming convention specified above:

```python
def asset_keys_to_dataset_urn_converter(
    self, asset_key: Sequence[str]
) -> Optional[DatasetUrn]:
    """
    Convert asset key to dataset urn

    By default, we assume the following asset key structure:
    key_prefix=["prod", "snowflake", "db_name", "schema_name"]
    """
    if len(asset_key) >= 3:
        return DatasetUrn(
            platform=asset_key[1],
            env=asset_key[0],
            name=".".join(asset_key[2:]),
        )
    else:
        return None
```

DataHub's Dagster integration can automatically detect dataset inputs and outputs for Software Defined Assets by analyzing the SQL queries it executes. To enable this feature, simply add the executed query to the Asset Metadata using the `Query` tag.

Here's an example of a Software Defined Asset with an annotated Query:

```python
@asset(key_prefix=["prod", "snowflake", "db_name", "schema_name"])
def my_asset_table_a(snowflake: SnowflakeResource) -> MaterializeResult:
    query = """
        create or replace table db_name.schema_name.my_asset_table_a as (
            SELECT *
            FROM db_name.schema_name.my_asset_table_b
        );
    """
    with snowflake.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
    return MaterializeResult(
        metadata={
            "Query": MetadataValue.text(query),
        }
    )
```

In this example, the plugin will automatically identify and set the upstream lineage as `db_name.schema_name.my_asset_table_b`.

Note: Proper asset naming is crucial, as the query parser determines the query language from the generated URN. In the example above, it will be `snowflake`.

For a complete example job, refer to the [iris.py file](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/dagster-plugin/examples/iris.py) in the DataHub repository.

### Extracting Lineage using SnowflakePandasIOManager

The plugin offers an extended version of base SnowflakePandasIOManager provided by Dagster called `DataHubSnowflakePandasIOManager`. This version automatically captures Snowflake assets created by the IO manager and adds DataHub URN and links to the assets in Dagster.

To use it, simply replace `SnowflakePandasIOManager` with `DataHubSnowflakePandasIOManager`. The enhanced version accepts two additional parameters:

1. `datahub_base_url`: The base URL of the DataHub UI, used to generate direct links to Snowflake Datasets in DataHub. If not set, no URL will be generated.
2. `datahub_env`: The DataHub environment to use when generating URNs. Defaults to `PROD` if not specified.

Example usage:

```python
from datahub_dagster_plugin.modules.snowflake_pandas.datahub_snowflake_pandas_io_manager import (
    DataHubSnowflakePandasIOManager,
)
# ...
resources={
    "snowflake_io_manager": DataHubSnowflakePandasIOManager(
        database="MY_DB",
        account="my_snowflake_account",
        warehouse="MY_WAREHOUSE",
        user="my_user",
        password="my_password",
        role="my_role",
        datahub_base_url="http://localhost:9002",
    ),
}
```

### Using Dagster Ins and Out

We can provide inputs and outputs to both Assets and Ops explicitly using a dictionary of `Ins` and `Out` corresponding to the decorated function arguments. While providing inputs and outputs, we can provide additional metadata as well.

To create dataset upstream and downstream dependency for the Assets and Ops, you can use an ins and out dictionary with metadata provided. For reference, look at the sample jobs created using assets [`assets_job.py`](../../metadata-ingestion-modules/dagster-plugin/examples/assets_job.py), or ops [`ops_job.py`](../../metadata-ingestion-modules/dagster-plugin/examples/ops_job.py).

### Using Custom Logic for Extracting Lineage

You can define your own logic to capture asset lineage information.

The output Tuple contains two dictionaries, one for input assets and the other for output assets. The key of the dictionary is the op key and the value is the set of asset URNs that are upstream or downstream of the op.

```python
from datahub_dagster_plugin.client.dagster_generator import DagsterGenerator, DatasetLineage

def asset_lineage_extractor(
    context: RunStatusSensorContext,
    dagster_generator: DagsterGenerator,
    graph: DataHubGraph,
) -> Dict[str, DatasetLineage]:
    dataset_lineage: Dict[str, DatasetLineage] = {}

    # Extracting input and output assets from the context
    return dataset_lineage
```

[See an example job here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/dagster-plugin/examples/advanced_ops_jobs.py).

## Debugging

### Connection Error for DataHub Rest URL

If you get `ConnectionError: HTTPConnectionPool(host='localhost', port=8080)`, then in that case your DataHub GMS service is not up.
