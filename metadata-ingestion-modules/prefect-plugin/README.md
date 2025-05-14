# prefect-datahub

Emit flows & tasks metadata to DataHub REST API with `prefect-datahub`

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

## Introduction

The `prefect-datahub` collection allows you to easily integrate DataHub's metadata ingestion capabilities into your Prefect workflows. With this collection, you can emit metadata about your flows, tasks, and workspace to DataHub's metadata service.

## Features

- Seamless integration with Prefect workflows
- Support for ingesting metadata of flows, tasks, and workspaces to DataHub GMS REST API
- Easy configuration using Prefect blocks

## Prerequisites

- Python 3.8+
- Prefect 2.0.0+ and < 3.0.0+
- A running instance of DataHub

## Installation

Install `prefect-datahub` using pip:

```bash
pip install prefect-datahub
```

We recommend using a Python virtual environment manager such as pipenv, conda, or virtualenv.

## Getting Started

### 1. Set up DataHub

Before using `prefect-datahub`, you need to deploy an instance of DataHub. Follow the instructions on the [DataHub Quickstart page](https://docs.datahub.com/docs/quickstart) to set up DataHub.

After successful deployment, the DataHub GMS service should be running on `http://localhost:8080` if deployed locally.

### 2. Configure DataHub Emitter

Save your DataHub configuration as a Prefect block:

```python
from prefect_datahub.datahub_emitter import DatahubEmitter

datahub_emitter = DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="DEV",
    platform_instance="local_prefect",
    token=None,  # generate auth token in the datahub and provide here if gms endpoint is secure
)
datahub_emitter.save("datahub-emitter-test")
```

Configuration options:

| Config            | Type  | Default                 | Description                                                                                                |
| ----------------- | ----- | ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| datahub_rest_url  | `str` | `http://localhost:8080` | DataHub GMS REST URL                                                                                       |
| env               | `str` | `PROD`                  | Environment for assets (see [FabricType](https://docs.datahub.com/docs/graphql/enums/#fabrictype))         |
| platform_instance | `str` | `None`                  | Platform instance for assets (see [Platform Instances](https://docs.datahub.com/docs/platform-instances/)) |

### 3. Use DataHub Emitter in Your Workflows

Here's an example of how to use the DataHub Emitter in a Prefect workflow:

```python
from prefect import flow, task
from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset

datahub_emitter_block = DatahubEmitter.load("datahub-emitter-test")

@task(name="Extract", description="Extract the data")
def extract():
    return "This is data"

@task(name="Transform", description="Transform the data")
def transform(data, datahub_emitter):
    transformed_data = data.split(" ")
    datahub_emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableX")],
        outputs=[Dataset("snowflake", "mydb.schema.tableY")],
    )
    return transformed_data

@flow(name="ETL", description="Extract transform load flow")
def etl():
    datahub_emitter = datahub_emitter_block
    data = extract()
    transformed_data = transform(data, datahub_emitter)
    datahub_emitter.emit_flow()

if __name__ == "__main__":
    etl()
```

**Note**: To emit task metadata, you must call `emit_flow()` at the end of your flow. Otherwise, no metadata will be emitted.

## Advanced Usage

For more advanced usage and configuration options, please refer to the [prefect-datahub documentation](https://docs.datahub.com/docs/lineage/prefect/).

## Contributing

We welcome contributions to `prefect-datahub`! Please refer to our [Contributing Guidelines](https://docs.datahub.com/docs/contributing) for more information on how to get started.

## Support

If you encounter any issues or have questions, you can:

- Open an issue in the [DataHub GitHub repository](https://github.com/datahub-project/datahub/issues)
- Join the [DataHub Slack community](https://datahubspace.slack.com)
- Seek help in the [Prefect Slack community](https://prefect.io/slack)

## License

`prefect-datahub` is released under the Apache 2.0 license. See the [LICENSE](https://github.com/datahub-project/datahub/blob/master/LICENSE) file for details.
