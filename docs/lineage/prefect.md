# Prefect Integration

DataHub supports integration of

- Prefect flow and task metadata
- Flow run and Task run information as well as
- Lineage information when present

## What is Prefect Datahub Block?

Blocks are primitive within Prefect that enable the storage of configuration and provide an interface for interacting with external systems. We integrated `prefect-datahub` block which use [Datahub Rest](../../metadata-ingestion/sink_docs/datahub.md#datahub-rest) emitter to emit metadata events while running prefect flow.

## Prerequisites to use Prefect Datahub Block

1. You need to use either Prefect Cloud (recommended) or the self hosted Prefect server.
2. Refer [Cloud Quickstart](https://docs.prefect.io/latest/getting-started/quickstart/) to setup Prefect Cloud.
3. Refer [Host Prefect server](https://docs.prefect.io/latest/guides/host/) to setup self hosted Prefect server.
4. Make sure the Prefect api url is set correctly. You can check it by running below command:
```shell
prefect profile inspect
```
5. If you are using Prefect Cloud, the API URL should be set as `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`.
6. If you are using a self-hosted Prefect server, the API URL should be set as `http://<host>:<port>/api`.

## Setup

### Installation

Install `prefect-datahub` with `pip`:

```shell
pip install 'prefect-datahub'
```

Requires an installation of Python 3.7+.

### Saving configurations to a block

This is a one-time activity, where you can save the configuration on the [Prefect block document store](https://docs.prefect.io/latest/concepts/blocks/#saving-blocks).
While saving you can provide below configurations. Default value will get set if not provided while saving the configuration to block.

Config | Type | Default | Description
--- | --- | --- | ---
datahub_rest_url | `str` | *http://localhost:8080* | DataHub GMS REST URL
env | `str` | *PROD* | The environment that all assets produced by this orchestrator belong to. For more detail and possible values refer [here](https://datahubproject.io/docs/graphql/enums/#fabrictype).
platform_instance | `str` | *None* | The instance of the platform that all assets produced by this recipe belong to. For more detail please refer [here](https://datahubproject.io/docs/platform-instances/).

```python
from prefect_datahub.datahub_emitter import DatahubEmitter
DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="PROD",
    platform_instance="local_prefect"
).save("BLOCK-NAME-PLACEHOLDER")
```

Congrats! You can now load the saved block to use your configurations in your Flow code:
 
```python
from prefect_datahub.datahub_emitter import DatahubEmitter
DatahubEmitter.load("BLOCK-NAME-PLACEHOLDER")
```

!!! info "Registering blocks"

    Register blocks in this module to
    [view and edit them](https://docs.prefect.io/ui/blocks/)
    on Prefect Cloud:

    ```bash
    prefect block register -m prefect_datahub
    ```

### Load the saved block in prefect workflows

After installing `prefect-datahub` and [saving the configution](#saving-configurations-to-a-block), you can easily use it within your prefect workflows to help you emit metadata event as show below!

```python
from prefect import flow, task
from prefect_datahub.dataset import Dataset
from prefect_datahub.datahub_emitter import DatahubEmitter

datahub_emitter = DatahubEmitter.load("MY_BLOCK_NAME")

@task(name="Transform", description="Transform the data")
def transform(data):
    data = data.split(" ")
    datahub_emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableA")],
        outputs=[Dataset("snowflake", "mydb.schema.tableC")],
    )
    return data

@flow(name="ETL flow", description="Extract transform load flow")
def etl():
    data = transform("This is data")
    datahub_emitter.emit_flow()
```

**Note**: To emit the tasks, user compulsory need to emit flow. Otherwise nothing will get emit.

## Concept mapping

Prefect concepts are documented [here](https://docs.prefect.io/latest/concepts/), and datahub concepts are documented [here](https://datahubproject.io/docs/what-is-datahub/datahub-concepts).

Prefect Concept | DataHub Concept
--- | ---
[Flow](https://docs.prefect.io/latest/concepts/flows/) | [DataFlow](https://datahubproject.io/docs/generated/metamodel/entities/dataflow/)
[Flow Run](https://docs.prefect.io/latest/concepts/flows/#flow-runs) | [DataProcessInstance](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance)
[Task](https://docs.prefect.io/latest/concepts/tasks/) | [DataJob](https://datahubproject.io/docs/generated/metamodel/entities/datajob/)
[Task Run](https://docs.prefect.io/latest/concepts/tasks/#tasks) | [DataProcessInstance](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance)
[Task Tag](https://docs.prefect.io/latest/concepts/tasks/#tags) | [Tag](https://datahubproject.io/docs/generated/metamodel/entities/tag/)


## How to validate saved block and emit of metadata

1. Go and check in Prefect UI at the Blocks menu if you can see the datahub emitter.
2. Run a Prefect workflow. In the flow logs, you should see Datahub related log messages like:

```
Emitting flow to datahub...
Emitting tasks to datahub...
```
## Debugging

### Incorrect Prefect API URL

If your Prefect API URL aren't being generated correctly or set incorrectly, then in that case you can set the Prefect API URL manually as show below:

```shell
prefect config set PREFECT_API_URL='http://127.0.0.1:4200/api'
```

### Connection error for Datahub Rest URL
If you get ConnectionError: HTTPConnectionPool(host='localhost', port=8080), then in that case your GMS service is not up.
