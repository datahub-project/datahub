<!-- PyPI long description. Keep concise, feature-discovery-first. -->

# DataHub Great Expectations (GX) Plugin

**Send Great Expectations data quality results into DataHub** — surface assertion outcomes alongside your dataset metadata so teams can see data health at a glance.

## What you can do

- **Emit assertion results** from GX Checkpoints directly into DataHub as data quality assertions
- **Link quality checks to datasets** — results appear on the dataset's profile in DataHub
- **Track pass/fail history** over time for every expectation suite
- **Works with any DataHub deployment** — self-hosted or DataHub Cloud

## Installation

```bash
pip install acryl-datahub-gx-plugin
```

## Quickstart

Add the DataHub action to your GX Checkpoint:

```python
from datahub_gx_plugin.action import DataHubValidationAction
from great_expectations.checkpoint import Checkpoint

checkpoint = Checkpoint(
    name="my_checkpoint",
    data_context=context,
    action_list=[
        {
            "name": "datahub",
            "action": {
                "class_name": "DataHubValidationAction",
                "module_name": "datahub_gx_plugin.action",
                "server_url": "http://localhost:8080",
            },
        }
    ],
)
```

Results from every Checkpoint run will appear in DataHub under the dataset's **Validation** tab.

## Links

- [Full documentation](/docs/metadata-ingestion/integration_docs/great-expectations)
- [Great Expectations](https://greatexpectations.io/)
- [GitHub](https://github.com/datahub-project/datahub)
- [Slack community](https://datahub.com/slack)
