<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub Prefect Plugin

**Automatic lineage and run metadata from Prefect into DataHub** — captures flow structure, task inputs/outputs, and run history with minimal setup.

## What you can do

- **Emit flow and task metadata** to DataHub as pipeline runs
- **Capture dataset lineage** — declare inputs and outputs per task and see them in DataHub
- **Configure via Prefect blocks** — store your DataHub connection settings as a reusable block
- **Works with any DataHub deployment** — self-hosted or DataHub Cloud

## Installation

```bash
pip install prefect-datahub
```

## Quickstart

### 1. Save your DataHub connection as a Prefect block

```python
from prefect_datahub.datahub_emitter import DatahubEmitter

DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="PROD",
).save("my-datahub")
```

### 2. Use it in your flows

```python
from prefect import flow, task
from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset

emitter = DatahubEmitter.load("my-datahub")

@task
def transform(data, emitter):
    emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.source_table")],
        outputs=[Dataset("snowflake", "mydb.schema.output_table")],
    )
    return data

@flow
def my_pipeline():
    data = extract()
    transform(data, emitter)
    emitter.emit_flow()   # required — emits all metadata at the end
```

## Configuration options

| Option              | Default                 | Description                         |
| ------------------- | ----------------------- | ----------------------------------- |
| `datahub_rest_url`  | `http://localhost:8080` | DataHub GMS URL                     |
| `env`               | `PROD`                  | Environment tag for assets          |
| `platform_instance` | `None`                  | Platform instance for assets        |
| `token`             | `None`                  | Auth token (if GMS auth is enabled) |

## Links

- [Full documentation](/docs/lineage/prefect)
- [Prefect](https://www.prefect.io/)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
