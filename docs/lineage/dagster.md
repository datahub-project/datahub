# Dagster Integration

DataHub supports the integration of

- Dagster Pipeline metadata
- Job and Op run information as well as
- Lineage information when present

## Using Datahub's Dagster Sensor

Dagster sensors allow us to perform some actions based on some state change. Datahub's defined dagster sensor will emit metadata after every dagster pipeline run execution. This sensor is able to emit both pipeline success as well as failures. For more details about Dagster sensors please refer [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors).

### Prerequisites

1. You need to create a new dagster project. See <https://docs.dagster.io/getting-started/create-new-project>.
2. There are two ways to define Dagster definition before starting dagster UI. One using [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions) class (recommended) and second using [Repositories](https://docs.dagster.io/concepts/repositories-workspaces/repositories#repositories).
3. Creation of new dagster project by default uses Definition class to define Dagster definition.

### Setup

1. You need to install the required dependency.

```shell
pip install acryl_datahub_dagster_plugin
```

2. You need to import DataHub dagster plugin provided sensor definition and add it in Dagster definition or dagster repository before starting dagster UI as show below:
**Using Definitions class:**

```python
{{ inline /metadata-ingestion-modules/dagster-plugin/examples/basic_setup.py }}
```

3. The DataHub dagster plugin provided sensor internally uses below configs. You can set these configs using environment variables. If not set, the sensor will take the default value.

   **Configuration options:**

   | Configuration Option          | Default value | Description                                                                                                                                                                                                                                                                                                                       |
   |-------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | datahub_client_config         |               | The DataHub client config                                                                                                                                                                                                                                                                                                         |
   | dagster_url                   |               | The url to your Dagster Webserver.                                                                                                                                                                                                                                                                                                |
   | capture_asset_materialization | True          | Whether to capture asset keys as Dataset on AssetMaterialization event                                                                                                                                                                                                                                                            |
   | capture_input_output          | True          | Whether to capture and try to parse input and output from HANDLED_OUTPUT,.LOADED_INPUT events. (currently only [PathMetadataValue](https://github.com/dagster-io/dagster/blob/7e08c05dcecef9fd07f887c7846bd1c9a90e7d84/python_modules/dagster/dagster/_core/definitions/metadata/__init__.py#L655) metadata supported (EXPERIMENTAL) |
   | platform_instance             |           | The instance of the platform that all assets produced by this recipe belong to. It is optional                                                                                                                                                                                                                                    |
   | asset_lineage_extractor             |           | You can implement your own logic to capture asset lineage information. See example for details[]                                                                                                                                                                                                                                  |
   | enable_asset_query_metadata_parsing             |           | Whether to enable parsing query from asset metadata. See below for details[]                                                                                                                                                                                                                                  |

4. Once Dagster UI is up, you need to turn on the provided sensor execution. To turn on the sensor, click on Overview tab and then on Sensors tab. You will see a toggle button in front of all defined sensors to turn it on/off.

5. DataHub dagster plugin provided sensor is ready to emit metadata after every dagster pipeline run execution.

### How to validate installation

1. Go and check in Dagster UI at Overview -> Sensors menu if you can see the 'datahub_sensor'.
2. Run a Dagster Job. In the dagster daemon logs, you should see DataHub related log messages like:

```
datahub_sensor - Emitting metadata...
```

## Dagster Ins and Out

We can provide inputs and outputs to both assets and ops explicitly using a dictionary of `Ins` and `Out` corresponding to the decorated function arguments. While providing inputs and outputs explicitly we can provide metadata as well.
To create dataset upstream and downstream dependency for the assets and ops you can use an ins and out dictionary with metadata provided. For reference, look at the sample jobs created using assets [`assets_job.py`](../../metadata-ingestion-modules/dagster-plugin/examples/assets_job.py), or ops [`ops_job.py`](../../metadata-ingestion-modules/dagster-plugin/examples/ops_job.py).

## Capturing Asset Dependencies Automatically

Dagster has no restrictions or conventions on asset naming and metadata properties.
Therefore, to automate metadata dependency extraction, we recommend and support the following opinionated approach.

1. Asset Naming:
    For asset naming, we recommend the following structure:
    `key_prefix=["env", "platform", "db_name", "schema_name"]`
    For example:

    ```python
    @asset(
         key_prefix=["prod", "snowflake", "db_name", "schema_name"], # the fqdn asset name to be able identify platform and make sure asset is unique
         deps=[iris_dataset],
    )
    ```

    If you properly name your Dagster asset, you can establish a proper connection between the asset and the dataset it is referring to. This allows for accurate tracking and lineage information, ensuring that the asset is correctly associated with its corresponding dataset.

    If you follow a different naming convention you can create your own `asset_keys_to_dataset_urn_converter` logic and set your callback.
    Here is the default implementation as example which can create DataHub urn from the Asset key naming above:

    ```python
        def asset_keys_to_dataset_urn_converter(
        self, asset_key: Sequence[str]
    ) -> Optional[DatasetUrn]:
        """
        Convert asset key to dataset urn

        By default we assume the following asset key structure:
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

2. Query Metadata:
    Datahub Dagster integration can automatically capture dataset inputs and outputs for Software Defined assets from the SQL queries it runs with SQL parsing if you add the query you run to the Asset Metadata with the `Query` tag.
    Here is an example Software Defined Asset which shows how you can add it:

    ```python
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
         return MaterializeResult( # Adding query to metadata to use it getting lineage from it with SQL parser
              metadata={
                    "Query": MetadataValue.text(query),
              }
         )
    ```

    For the above example, the plugin will automatically extract and set the Upstream of `db_name.schema_name.my_asset_table_b`.

[See a full example job here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/dagster-plugin/examples/iris.py).

## Define your custom logic to capture asset lineage information

You can define your own logic to capture asset lineage information.

The output Tuple contains two dictionaries, one for input assets and the other for output assets. The key of the dictionary is the op key and the value is the set of asset urns that are upstream or downstream of the op.

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

[See example job here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion-modules/dagster-plugin/examples/advanced_ops_jobs.py).

## Debugging

### Connection error for Datahub Rest URL

If you get ConnectionError: HTTPConnectionPool(host='localhost', port=8080), then in that case your DataHub GMS service is not up.
