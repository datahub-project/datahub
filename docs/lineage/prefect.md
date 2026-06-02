---
description: "Capture and emit dataset lineage and run metadata from Prefect flows and tasks into DataHub for end-to-end pipeline visibility."
---

# Prefect Integration with DataHub

## Overview

DataHub supports integration with Prefect, allowing you to ingest:

- Prefect flow and task metadata
- Flow run and Task run information
- Lineage information (when available)

This integration enables you to track and monitor your Prefect workflows within DataHub, providing a comprehensive view of your data pipeline activities.

## Prefect DataHub Block

### What is a Prefect DataHub Block?

Blocks in Prefect are primitives that enable the storage of configuration and provide an interface for interacting with external systems. The `prefect-datahub` block uses the [DataHub REST](../../metadata-ingestion/sink_docs/datahub.md#datahub-rest) emitter to send metadata events while running Prefect flows.

### Prerequisites

1. Use either Prefect Cloud (recommended) or a self-hosted Prefect server.
2. For Prefect Cloud setup, refer to the [Cloud Quickstart](https://docs.prefect.io/latest/getting-started/quickstart/) guide.
3. For self-hosted Prefect server setup, refer to the [Host Prefect Server](https://docs.prefect.io/latest/guides/host/) guide.
4. Ensure the Prefect API URL is set correctly. Verify using:

   ```shell
   prefect profile inspect
   ```

5. API URL format:
   - Prefect Cloud: `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`
   - Self-hosted: `http://<host>:<port>/api`

## Upgrading from Prefect v2

`prefect-datahub` ≥ 1.0.0 requires **Prefect v3** and contains two breaking changes for existing users.

**Module path changed.** The emitter moved from `prefect_datahub.prefect_datahub` to `prefect_datahub.datahub_emitter`. Update any direct imports:

```python
# Before (Prefect v2)
from prefect_datahub.prefect_datahub import DatahubEmitter

# After (Prefect v3)
from prefect_datahub.datahub_emitter import DatahubEmitter
```

**Saved blocks must be re-registered.** Blocks previously saved to Prefect Cloud or a self-hosted server under the old entry point (`prefect_datahub.prefect_datahub:DatahubEmitter`) will fail to load after the upgrade because that module no longer exists. Re-register by running `.save()` again:

```python
from prefect_datahub.datahub_emitter import DatahubEmitter

DatahubEmitter(datahub_rest_url="...", env="PROD").save("MY-DATAHUB-BLOCK", overwrite=True)
```

In Prefect v3, block types are discovered automatically when the collection package is imported — the entry point no longer needs to reference the class explicitly.

## Setup Instructions

### 1. Installation

Install `prefect-datahub` using pip:

```shell
pip install 'prefect-datahub'
```

Note: Requires Python 3.10+ and Prefect 3.x (`>=3.0.0,<4.0.0`). Prefect 2.x is not supported. If you are upgrading from Prefect 2.x, upgrade Prefect first and re-register the DataHub block (see [Upgrading from Prefect v2](#upgrading-from-prefect-v2)).

### 2. Saving Configurations to a Block

Save your configuration to the [Prefect block document store](https://docs.prefect.io/latest/concepts/blocks/#saving-blocks):

```python
from prefect_datahub.datahub_emitter import DatahubEmitter

DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="PROD",
    platform_instance="local_prefect"
).save("MY-DATAHUB-BLOCK")
```

Configuration options:

| Config            | Type  | Default                 | Description                                                                                                |
| ----------------- | ----- | ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| datahub_rest_url  | `str` | `http://localhost:8080` | DataHub GMS REST URL                                                                                       |
| env               | `str` | `PROD`                  | Environment for assets (see [FabricType](https://docs.datahub.com/docs/graphql/enums/#fabrictype))         |
| platform_instance | `str` | `None`                  | Platform instance for assets (see [Platform Instances](https://docs.datahub.com/docs/platform-instances/)) |

### 3. Using the Block in Prefect Workflows

Load and use the saved block in your Prefect workflows:

```python
from prefect import flow, task
from prefect_datahub.dataset import Dataset
from prefect_datahub.datahub_emitter import DatahubEmitter

datahub_emitter = DatahubEmitter.load("MY-DATAHUB-BLOCK")

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

**Note**: To emit tasks, you must call `emit_flow()`. Otherwise, no metadata will be emitted.

## Concept Mapping

| Prefect Concept                                                      | DataHub Concept                                                                                       |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [Flow](https://docs.prefect.io/latest/concepts/flows/)               | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)                      |
| [Flow Run](https://docs.prefect.io/latest/concepts/flows/#flow-runs) | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance) |
| [Task](https://docs.prefect.io/latest/concepts/tasks/)               | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                        |
| [Task Run](https://docs.prefect.io/latest/concepts/tasks/#tasks)     | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance) |
| [Task Tag](https://docs.prefect.io/latest/concepts/tasks/#tags)      | [Tag](https://docs.datahub.com/docs/generated/metamodel/entities/tag/)                                |

## Validation and Troubleshooting

### Validating the Setup

1. Check the Prefect UI's Blocks menu for the DataHub emitter.
2. Run a Prefect workflow and look for DataHub-related log messages:

   ```text
   Emitting flow to datahub...
   Emitting tasks to datahub...
   ```

### Debugging Common Issues

#### Incorrect Prefect API URL

If the Prefect API URL is incorrect, set it manually:

```shell
prefect config set PREFECT_API_URL='http://127.0.0.1:4200/api'
```

#### DataHub Connection Error

If you encounter a `ConnectionError: HTTPConnectionPool(host='localhost', port=8080)`, ensure that your DataHub GMS service is running.

## Additional Resources

- [Prefect Documentation](https://docs.prefect.io/)
- [DataHub Documentation](https://docs.datahub.com/docs/)

For more information or support, please refer to the official Prefect and DataHub documentation or reach out to their respective communities.
