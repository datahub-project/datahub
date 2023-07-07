# Dagster Integration

DataHub supports integration of

- Dagster Pipeline metadata
- Job and Op run information as well as
- Lineage information when present

## Using Datahub's Dagster Sensor

Dagster sensors allow us to perform some action based on some state change. Datahub's defined dagster sensor will emits metadata after every dagster pipeline run execution. This sensor is able to register both pipeline success as well as failures. For more details about Dagster sensors please refer [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors).

## Prerequisites

1. You need to create a new dagster project. See <https://docs.dagster.io/getting-started/create-new-project>.
2. There are two ways to define Dagster definition before starting dagster UI. One using [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions) class (recommended) and second using [Repositories](https://docs.dagster.io/concepts/repositories-workspaces/repositories#repositories).
3. Creation of new dagster project by default use Definition class to define Dagster definition.

### Setup

1. You need to install the required dependency.

```shell
pip install acryl-datahub[dagster]
```

2. You need to import DataHub provided sensor definition and add it in Dagster definition or dagster repository before starting dagster UI as show below: 
**Using Definitions class:**

```python
from dagster import Definitions
from datahub_provider.sensors.datahub_sensors import datahub_sensor

defs = Definitions(
    sensors=[datahub_sensor],
)
```

**Using Repository decorator:**

```python
from dagster import repository
from datahub_provider.sensors.datahub_sensors import datahub_sensor

@repository
def my_repository():
    return [datahub_sensor]
```

3. The DataHub provided sensor internally use below configs. You can set this configs using environment variables. If not set, sensor will take default value.

   **Configuration options:**

   | Name                           | Default value         | Environment variable key  | Description                                                                                   |
   | ------------------------------ | --------------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
   | datahub_rest_url               | http://localhost:8080 | DATAHUB_REST_URL          | Datahub GMS Rest URL where datahub event get emitted.                                         |
   | env                            | PROD                  | DATAHUB_ENV               | The environment that all assets produced by this connector belong to.                         |
   | platform_instance              | None                  | DATAHUB_PLATFORM_INSTANCE | The instance of the platform that all assets produced by this recipe belong to.               |

5. You can configure DataHub `inputs` and `outputs` for your Dagster assets and ops using `ins` and `out` metadata. For reference, look at the below sample dagster assets and ops definition:

```python
from dagster import asset, multi_asset, op, In, Out, Output, AssetIn, AssetOut
from datahub_provider.entities import Dataset

@multi_asset(outs={"my_asset": AssetOut(
    metadata={"datahub.inputs": [Dataset('snowflake', 'tableC').urn]}
)}) 
def my_asset():
    data = [1, 2, 3]
    metadata = {
        "num_records": len(data),
    }
    return Output(value=data, metadata=metadata)

@asset(ins={"my_asset": AssetIn(
    key="my_asset", 
    metadata={"datahub.inputs": [Dataset('snowflake', 'tableA').urn]}
)})
def my_secound_asset(my_asset):
    return my_asset.extend([4,5])    

@op(ins={"name": In(metadata={"datahub.inputs": [Dataset('snowflake', 'tableA').urn]})},
    out={"result": Out(metadata={"datahub.outputs": [Dataset('snowflake', 'tableB').urn]})})
def my_op(name):
    return "hello" + name
```

6. Once Dagster UI is up, you need to turn on the DataHub provided sensor execution. To turn on the sensor, click on Overview tab and then on Sensors tab. You will see to toggle button in front of all defined sensors to turn it on/off.

7. DataHub provided sensor is ready emit metadata after every dagster pipeline run execution.

### How to validate installation

1. Go and check in Dagster UI at Overview -> Sensors menu if you can see the 'datahub_sensor'.
2. Run an Dagster Job. In the dagster deamon logs, you should see DataHub related log messages like:

```
datahub_sensor - Emitting metadata...
```

## Debugging

### Connection error for Datahub Rest URL
If you get ConnectionError: HTTPConnectionPool(host='localhost', port=8080), then in that case your DataHub GMS service is not up.
