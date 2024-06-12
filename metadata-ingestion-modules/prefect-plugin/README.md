# Emit flows & tasks metadata to DataHub rest with `prefect-datahub`

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-datahub/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-datahub?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/datahub-project/datahub/" alt="Stars">
        <img src="https://img.shields.io/github/stars/datahub-project/datahub?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-datahub/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-datahub?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/datahub-project/datahub/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/datahub-project/datahub?color=0052FF&labelColor=090422" /></a>
    <br/>
    <a href="https://datahubspace.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
</p>

## Welcome!

The `prefect-datahub` collection makes it easy to leverage the capabilities of DataHub emitter in your flows, featuring support for ingesting metadata of flows, tasks & workspace to DataHub gms rest.


## Getting Started

### Setup DataHub UI

In order to use 'prefect-datahub' collection, you'll first need to deploy the new instance of DataHub. 

You can get the instructions on deploying the open source DataHub by navigating to the [apps page](https://datahubproject.io/docs/quickstart).

Successful deployment of DataHub will lead creation of DataHub GMS service running on 'http://localhost:8080' if you have deployed it on local system.

### Saving configurations to a block


This is a one-time activity, where you can save the configuration on the [Prefect block document store](https://docs.prefect.io/2.10.13/concepts/blocks/#saving-blocks).
While saving you can provide below configurations. Default value will get set if not provided while saving the configuration to block.

Config | Type | Default | Description
--- | --- | --- | ---
datahub_rest_url | `str` | *http://localhost:8080* | DataHub GMS REST URL
env | `str` | *PROD* | The environment that all assets produced by this orchestrator belong to. For more detail and possible values refer [here](https://datahubproject.io/docs/graphql/enums/#fabrictype).
platform_instance | `str` | *None* | The instance of the platform that all assets produced by this recipe belong to. For more detail please refer [here](https://datahubproject.io/docs/platform-instances/).

```python
import asyncio
from prefect_datahub.datahub_emitter import DatahubEmitter


async def save_datahub_emitter():
    datahub_emitter = DatahubEmitter(
        datahub_rest_url="http://localhost:8080",
        env="PROD",
        platform_instance="local_prefect",
    )

    await datahub_emitter.save("datahub-block-7", overwrite=True)


asyncio.run(save_datahub_emitter())
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
import asyncio

from prefect import flow, task

from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset


async def load_datahub_emitter():
    datahub_emitter = DatahubEmitter()
    return datahub_emitter.load("datahub-block-7")


@task(name="Extract", description="Extract the data")
def extract():
    data = "This is data"
    return data


@task(name="Transform", description="Transform the data")
def transform(data, datahub_emitter):
    data = data.split(" ")
    datahub_emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableX")],
        outputs=[Dataset("snowflake", "mydb.schema.tableY")],
    )
    return data


@flow(name="ETL", description="Extract transform load flow")
def etl():
    datahub_emitter = asyncio.run(load_datahub_emitter())
    data = extract()
    data = transform(data, datahub_emitter)
    datahub_emitter.emit_flow()


etl()
```

**Note**: To emit the tasks, user compulsory need to emit flow. Otherwise nothing will get emit.

## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Installation

Install `prefect-datahub` with `pip`:

```bash
pip install prefect-datahub
```

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0.0 or higher. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Feedback

If you encounter any bugs while using `prefect-datahub`, feel free to open an issue in the [datahub](https://github.com/datahub-project/datahub) repository.

If you have any questions or issues while using `prefect-datahub`, you can find help in the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`datahub`](https://github.com/datahub-project/datahub) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-datahub`, please refer to our [Contributing Guidelines](https://datahubproject.io/docs/contributing).
