# Dagster Integration

DataHub supports integration of

- Dagster Pipeline metadata
- Job and Op run information as well as
- Lineage information when present

## Using Datahub's Dagster Sensor

Dagster sensors allow us to perform some action based on some state change. Datahub's defined dagster sensor will emit metadata after every dagster pipeline run execution. This sensor is able to emit both pipeline success as well as failures. For more details about Dagster sensors please refer [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors).

### Prerequisites

1. You need to create a new dagster project. See <https://docs.dagster.io/getting-started/create-new-project>.
2. There are two ways to define Dagster definition before starting dagster UI. One using [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions) class (recommended) and second using [Repositories](https://docs.dagster.io/concepts/repositories-workspaces/repositories#repositories).
3. Creation of new dagster project by default uses Definition class to define Dagster definition.

### Setup

1. You need to install the required dependency.

```shell
pip install acryl-datahub[dagster]
```

2. You need to import DataHub dagster plugin provided sensor definition and add it in Dagster definition or dagster repository before starting dagster UI as show below: 
**Using Definitions class:**

```python
from dagster import Definitions
from datahub_dagster_plugin.sensors.datahub_sensors import datahub_sensor

defs = Definitions(
    sensors=[datahub_sensor],
)
```

**Using Repository decorator:**

```python
from dagster import repository
from datahub_dagster_plugin.sensors.datahub_sensors import datahub_sensor

@repository
def my_repository():
    return [datahub_sensor]
```

3. The DataHub dagster plugin provided sensor internally uses below configs. You can set these configs using environment variables. If not set, the sensor will take the default value.

   **Configuration options:**

   | Environment variable           | Default value         | Description                                                                                   |
   | ------------------------------ | --------------------- | --------------------------------------------------------------------------------------------- |
   | DATAHUB_REST_URL               | http://localhost:8080 | Datahub GMS Rest URL where datahub events get emitted.                                        |
   | DATAHUB_ENV                    | PROD                  | The environment that all assets produced by this connector belong to.                         |
   | DATAHUB_PLATFORM_INSTANCE      | None                  | The instance of the platform that all assets produced by this recipe belong to.               |

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
To create dataset upstream and downstream dependency for the assets and ops you can use an ins and out dictionary with metadata provided. For reference, look at the sample jobs created using assets [`assets_job.py`](../../metadata-ingestion-modules/dagster-plugin/src/datahub_dagster_plugin/example_jobs/assets_job.py), or ops [`ops_job.py`](../../metadata-ingestion-modules/dagster-plugin/src/datahub_dagster_plugin/example_jobs/ops_job.py).



## Debugging

### Connection error for Datahub Rest URL
If you get ConnectionError: HTTPConnectionPool(host='localhost', port=8080), then in that case your DataHub GMS service is not up.
