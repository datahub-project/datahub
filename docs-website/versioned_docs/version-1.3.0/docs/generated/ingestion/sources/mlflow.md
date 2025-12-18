---
sidebar_position: 42
title: MLflow
slug: /generated/ingestion/sources/mlflow
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/mlflow.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MLflow
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Extract ML experiments. Supported for types - ML Experiment. |
| Descriptions | ✅ | Extract descriptions for MLflow Registered Models and Model Versions. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Tags | ✅ | Extract tags for MLflow Registered Model Stages. |


:::note Version Compatbility

This connector requires an MLflow server version **1.28.0 or later**.  
If you're using an earlier version, ingestion of **Experiments** and **Runs** will be skipped.

:::

### Concept Mapping

This ingestion source maps the following MLflow Concepts to DataHub Concepts:

|                                        Source Concept                                         |                                             DataHub Concept                                              | Notes                                                                                                                                                                                                                                                                                                |
| :-------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|     [`Registered Model`](https://mlflow.org/docs/latest/model-registry/#registered-model)     |        [`MlModelGroup`](/docs/generated/metamodel/entities/mlmodelgroup/)        | The name of a Model Group is the same as a Registered Model's name (e.g. my_mlflow_model). Registered Models serve as containers for multiple versions of the same model in MLflow.                                                                                                                  |
|        [`Model Version`](https://mlflow.org/docs/latest/model-registry/#model-version)        |             [`MlModel`](/docs/generated/metamodel/entities/mlmodel/)             | The name of a Model is `{registered_model_name}{model_name_separator}{model_version}` (e.g. my_mlflow_model_1 for Registered Model named my_mlflow_model and Version 1, my_mlflow_model_2, etc.). Each Model Version represents a specific iteration of a model with its own artifacts and metadata. |
|             [`Experiment`](https://mlflow.org/docs/latest/tracking/#experiments)              |           [`Container`](/docs/generated/metamodel/entities/container/)           | Each Experiment in MLflow is mapped to a Container in DataHub. Experiments organize related runs and serve as logical groupings for model development iterations, allowing tracking of parameters, metrics, and artifacts.                                                                           |
|                    [`Run`](https://mlflow.org/docs/latest/tracking/#runs)                     | [`DataProcessInstance`](/docs/generated/metamodel/entities/dataprocessinstance/) | Captures the run's execution details, parameters, metrics, and lineage to a model.                                                                                                                                                                                                                   |
| [`Model Stage`](https://mlflow.org/docs/latest/model-registry/#deprecated-using-model-stages) |                 [`Tag`](/docs/generated/metamodel/entities/tag/)                 | The mapping between Model Stages and generated Tags is the following:<br/>- Production: mlflow_production<br/>- Staging: mlflow_staging<br/>- Archived: mlflow_archived<br/>- None: mlflow_none. Model Stages indicate the deployment status of each version.                                        |

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: mlflow
  config:
    # Coordinates
    tracking_uri: tracking_uri

sink:
  # sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_external_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Base URL to use when constructing external URLs to MLflow. If not set, tracking_uri is used if it's an HTTP URL. If neither is set, external URLs are not generated. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">materialize_dataset_inputs</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to materialize dataset inputs for each run <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">model_name_separator</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | A string which separates model name from its version (e.g. model_1 or model-1) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#95;</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Password for MLflow authentication <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">registry_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Registry server URI. If not set, an MLflow default registry_uri is used (value of tracking_uri or `MLFLOW_REGISTRY_URI` environment variable) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">source_mapping_to_platform</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Mapping of source type to datahub platform <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tracking_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Tracking server URI. If not set, an MLflow default tracking_uri is used (local `mlruns/` directory or `MLFLOW_TRACKING_URI` environment variable) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Username for MLflow authentication <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "tracking_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tracking server URI. If not set, an MLflow default tracking_uri is used (local `mlruns/` directory or `MLFLOW_TRACKING_URI` environment variable)",
      "title": "Tracking Uri"
    },
    "registry_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Registry server URI. If not set, an MLflow default registry_uri is used (value of tracking_uri or `MLFLOW_REGISTRY_URI` environment variable)",
      "title": "Registry Uri"
    },
    "model_name_separator": {
      "default": "_",
      "description": "A string which separates model name from its version (e.g. model_1 or model-1)",
      "title": "Model Name Separator",
      "type": "string"
    },
    "base_external_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Base URL to use when constructing external URLs to MLflow. If not set, tracking_uri is used if it's an HTTP URL. If neither is set, external URLs are not generated.",
      "title": "Base External Url"
    },
    "materialize_dataset_inputs": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Whether to materialize dataset inputs for each run",
      "title": "Materialize Dataset Inputs"
    },
    "source_mapping_to_platform": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Mapping of source type to datahub platform",
      "title": "Source Mapping To Platform"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Username for MLflow authentication",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Password for MLflow authentication",
      "title": "Password"
    }
  },
  "title": "MLflowConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

### Auth Configuration

You can configure the MLflow source to authenticate with the MLflow server using the `username` and `password` configuration options.

```yaml
source:
  type: mlflow
  config:
    tracking_uri: "http://127.0.0.1:5000"
    username: <username>
    password: <password>
```

### Dataset Lineage

You can map MLflow run datasets to specific DataHub platforms using the `source_mapping_to_platform` configuration option. This allows you to specify which DataHub platform should be associated with datasets from different MLflow engines.

Example:

```yaml
source_mapping_to_platform:
  huggingface: snowflake # Maps Hugging Face datasets to Snowflake platform
  http: s3 # Maps HTTP data sources to s3 platform
```

By default, DataHub will attempt to connect lineage with existing datasets based on the platform and name, but will not create new datasets if they don't exist.

To enable automatic dataset creation and lineage mapping, use the `materialize_dataset_inputs` option:

```yaml
materlize_dataset_inputs: true # Creates new datasets if they don't exist
```

You can configure these options independently:

```yaml
# Only map to existing datasets
materlize_dataset_inputs: false
source_mapping_to_platform:
    huggingface: snowflake  # Maps Hugging Face datasets to Snowflake platform
    pytorch: snowflake      # Maps PyTorch datasets to Snowflake platform

# Create new datasets and map platforms
materlize_dataset_inputs: true
source_mapping_to_platform:
    huggingface: snowflake
    pytorch: snowflake
```

### Code Coordinates
- Class Name: `datahub.ingestion.source.mlflow.MLflowSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mlflow.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for MLflow, feel free to ping us on [our Slack](https://datahub.com/slack).
