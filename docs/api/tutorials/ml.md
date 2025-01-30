import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AI/ML Framework Integration with DataHub

## Why Integrate Your AI/ML System with DataHub?

As a data practitioner, keeping track of your AI experiments, models, and their relationships can be challenging. 
DataHub makes this easier by providing a central place to organize and track your AI assets.

This guide will show you how to integrate your AI workflows with DataHub. 
With integrations for popular ML platforms like MLflow and Amazon SageMaker, DataHub enables you to easily find and share AI models across your organization, track how models evolve over time, and understand how training data connects to each model.
Most importantly, it enables seamless collaboration on AI projects by making everything discoverable and connected.

## Goals Of This Guide

In this guide, you'll learn how to:
- Create your basic AI components (models, experiments, runs)
- Connect these components to build a complete AI system
- Track relationships between models, data, and experiments

## Core AI Concepts

Here's what you need to know about the key components in DataHub:

- **Experiments** are collections of training runs for the same project, like all attempts to build a churn predictor
- **Training Runs** are attempts to train a model within an experiment, capturing parameters and results
- **Model Groups** organize related models together, like all versions of your churn predictor
- **Models** are successful training runs registered for production use

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/concept-diagram-dh-term.png"/>
</p>

The hierarchy works like this:
1. Every run belongs to an experiment
2. Successful runs can be registered as models
3. Models belong to a model group
4. Not every run becomes a model

:::note Terminology Mapping
Different AI platforms (MLflow, Amazon SageMaker) have their own terminology. 
To keep things consistent, we'll use DataHub's terms throughout this guide.
Here's how DataHub's terminology maps to these platforms:

| DataHub | Description                         | MLflow | SageMaker |
|---------|-------------------------------------|---------|-----------|
| ML Model Group | Collection of related models        | Model | Model Group |
| ML Model | Versioned artifact in a model group | Model Version | Model Version |
| ML Training Run | Single training attempt             | Run | Training Job |
| ML Experiment | Collection of training runs         | Experiment | Experiment |
:::

For platform-specific details, see our integration guides for [MLflow](/docs/generated/ingestion/sources/mlflow.md) and [Amazon SageMaker](/docs/generated/ingestion/sources/sagemaker.md).

## Basic Setup

To follow this tutorial, you'll need DataHub Quickstart deployed locally.
For detailed steps, see the [Datahub Quickstart Guide](/docs/quickstart.md).

Next, set up the Python client for DataHub using `DatahubAIClient`defined in [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/ai/dh_ai_client.py).

Create a token in DataHub UI and replace `<your_token>` with your token:

```python
from dh_ai_client import DatahubAIClient

client = DatahubAIClient(token="<your_token>", server_url="http://localhost:9002")
```

:::note Verifying via GraphQL
Throughout this guide, we'll show how to verify changes using GraphQL queries.
You can run these queries in the DataHub UI at `https://localhost:9002/api/graphiql`.
:::

## Create Simple AI Assets

Let's create the basic building blocks of your ML system. These components will help you organize your AI work and make it discoverable by your team.

### Create a Model Group

A model group contains different versions of a similar model. For example, all versions of your "Customer Churn Predictor" would go in one group.

<Tabs>
<TabItem value="basic" label="Basic">
Create a basic model group with just an identifier:

```python
client.create_model_group(
    group_id="airline_forecast_models_group",
)
```

</TabItem>
<TabItem value="advanced" label="Advanced">
Add rich metadata like descriptions, creation timestamps, and team information:

```python
client.create_model_group(
    group_id="airline_forecast_models_group",
    properties=models.MLModelGroupPropertiesClass(
        name="Airline Forecast Models Group",
        description="Group of models for airline passenger forecasting",
        created=models.TimeStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
    ),
)
```

</TabItem>
</Tabs>

Let's verify that our model group was created:

<Tabs>
<TabItem value="UI" label="UI">
See your new model group in the DataHub UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/model-group-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query your model group to check its properties:

```graphql
query {
  mlModelGroup(
    urn:"urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,airline_forecast_models_group,PROD)"
  ) {
    name
    description
  }
}
```

The response will show your model group's details:

```json
{
  "data": {
    "mlModelGroup": {
      "name": "airline_forecast_models_group",
      "description": "Group of models for airline passenger forecasting"
    }
  }
}
```

</TabItem>
</Tabs>

### Create a Model

Next, let's create a specific model version that represents a trained model ready for deployment.

<Tabs>
<TabItem value="basic" label="Basic">
Create a model with just the required version:

```python
client.create_model(
    model_id="arima_model",
    version="1.0",
)
```

</TabItem>
<TabItem value="advanced" label="Advanced">
Include metrics, parameters, and metadata for production use:

```python
client.create_model(
    model_id="arima_model",
    properties=models.MLModelPropertiesClass(
        name="ARIMA Model",
        description="ARIMA model for airline passenger forecasting",
        customProperties={"team": "forecasting"},
        trainingMetrics=[
            models.MLMetricClass(name="accuracy", value="0.9"),
            models.MLMetricClass(name="precision", value="0.8"),
        ],
        hyperParams=[
            models.MLHyperParamClass(name="learning_rate", value="0.01"),
            models.MLHyperParamClass(name="batch_size", value="32"),
        ],
        externalUrl="https:localhost:5000",
        created=models.TimeStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
        tags=["forecasting", "arima"],
    ),
    version="1.0",
    alias="champion",
)
```

</TabItem>
</Tabs>

Let's verify our model:

<Tabs>
<TabItem value="UI" label="UI">
Check your model's details in the DataHub UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/model-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query your model's information:

```graphql
query {
  mlModel(
    urn:"urn:li:mlModel:(urn:li:dataPlatform:mlflow,arima_model,PROD)"
  ) {
    name
    description
    versionProperties {
      version {
        versionTag
      }
    }
  }
}
```

The response will show your model's details:

```json
{
  "data": {
    "mlModel": {
      "name": "arima_model",
      "description": "ARIMA model for airline passenger forecasting",
      "versionProperties": {
        "version": {
          "versionTag": "1.0"
        }
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Create an Experiment

An experiment helps organize multiple training runs for a specific project.

<Tabs>
<TabItem value="basic" label="Basic">
Create a basic experiment:

```python
client.create_experiment(
    experiment_id="airline_forecast_experiment",
)
```

</TabItem>
<TabItem value="advanced" label="Advanced">
Add context and metadata:

```python
client.create_experiment(
    experiment_id="airline_forecast_experiment",
    properties=models.ContainerPropertiesClass(
        name="Airline Forecast Experiment",
        description="Experiment to forecast airline passenger numbers",
        customProperties={"team": "forecasting"},
        created=models.TimeStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
    ),
)
```

</TabItem>
</Tabs>

Verify your experiment:

<Tabs>
<TabItem value="UI" label="UI">
See your experiment's details in the UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/experiment-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query your experiment's information:

```graphql
query {
  container(
    urn:"urn:li:container:airline_forecast_experiment"
  ) {
    name
    description
    properties {
      customProperties
    }
  }
}
```

Check the response:

```json
{
  "data": {
    "container": {
      "name": "Airline Forecast Experiment",
      "description": "Experiment to forecast airline passenger numbers",
      "properties": {
        "customProperties": {
          "team": "forecasting"
        }
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Create a Training Run

A training run captures all details about a specific model training attempt.

<Tabs>
<TabItem value="basic" label="Basic">
Create a basic training run:

```python
client.create_training_run(
    run_id="simple_training_run_4",
)
```

</TabItem>
<TabItem value="advanced" label="Advanced">
Include metrics, parameters, and other important metadata:

```python
client.create_training_run(
    run_id="simple_training_run_4",
    properties=models.DataProcessInstancePropertiesClass(
        name="Simple Training Run 4",
        created=models.AuditStampClass(
            time=1628580000000, actor="urn:li:corpuser:datahub"
        ),
        customProperties={"team": "forecasting"},
    ),
    training_run_properties=models.MLTrainingRunPropertiesClass(
        id="simple_training_run_4",
        outputUrls=["s3://my-bucket/output"],
        trainingMetrics=[models.MLMetricClass(name="accuracy", value="0.9")],
        hyperParams=[models.MLHyperParamClass(name="learning_rate", value="0.01")],
        externalUrl="https:localhost:5000",
    ),
    run_result=RunResultType.FAILURE,
    start_timestamp=1628580000000,
    end_timestamp=1628580001000,
)
```

</TabItem>
</Tabs>

Verify your training run:

<Tabs>
<TabItem value="UI" label="UI">
View the run details in the UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query your training run:

```graphql
query {
  dataProcessInstance(
    urn:"urn:li:dataProcessInstance:simple_training_run_4"
  ) {
    name
    created {
      time
    }
    properties {
      customProperties
    }
  }
}
```

Check the response:

```json
{
  "data": {
    "dataProcessInstance": {
      "name": "Simple Training Run 4",
      "created": {
        "time": 1628580000000
      },
      "properties": {
        "customProperties": {
          "team": "forecasting"
        }
      }
    }
  }
}
```

</TabItem>
</Tabs>

## Define Entity Relationships

Now let's connect these components to create a comprehensive ML system. These connections enable you to track model lineage, monitor model evolution, understand dependencies, and search effectively across your ML assets.

### Add Model To Model Group

Connect your model to its group:

```python
client.add_model_to_model_group(model_urn=model_urn, group_urn=model_group_urn)
```

<Tabs>
<TabItem value="UI" label="UI">

View model versions in the **Model Group** under the **Models** section:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/model-group-with-model.png"/>
</p>

Find group information in the **Model** page under the **Group** tab:
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/model-with-model-group.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query the model-group relationship:

```graphql
query {
  mlModel(
    urn:"urn:li:mlModel:(urn:li:dataPlatform:mlflow,arima_model,PROD)"
  ) {
    name
    properties {
      groups {
        urn
        properties {
          name
        }
      }
    }
  }
}
```

Check the response:

```json
{
  "data": {
    "mlModel": {
      "name": "arima_model",
      "properties": {
        "groups": [
          {
            "urn": "urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,airline_forecast_model_group,PROD)",
            "properties": {
              "name": "Airline Forecast Model Group"
            }
          }
        ]
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Add Run To Experiment

Connect a training run to its experiment:

```python
client.add_run_to_experiment(run_urn=run_urn, experiment_urn=experiment_urn)
```

<Tabs>
<TabItem value="UI" label="UI">

Find your runs in the **Experiment** page under the **Entities** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/experiment-with-run.png"/>
</p>

See the experiment details in the **Run** page:
<p align="center">
  <img width="40%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-with-experiment.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query the run-experiment relationship:

```graphql
query {
  dataProcessInstance(
    urn:"urn:li:dataProcessInstance:simple_training_run"
  ) {
    name
    parentContainers {
      containers {
        urn
        properties {
          name
        }
      }
    }
  }
}
```

View the relationship details:

```json
{
  "data": {
    "dataProcessInstance": {
      "name": "Simple Training Run",
      "parentContainers": {
        "containers": [
          {
            "urn": "urn:li:container:airline_forecast_experiment",
            "properties": {
              "name": "Airline Forecast Experiment"
            }
          }
        ]
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Add Run To Model

Connect a training run to its resulting model:

```python
client.add_run_to_model(model_urn=model_urn, run_urn=run_urn)
```

This relationship enables you to:
- Track which runs produced each model
- Understand model provenance
- Debug model issues
- Monitor model evolution

<Tabs>
<TabItem value="UI" label="UI">

Find the source run in the **Model** page under the **Summary** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/model-with-source-run.png"/>
</p>

See related models in the **Run** page under the **Lineage** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-lineage-model.png"/>
</p>
<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-lineage-model-graph.png"/>
</p>

</TabItem>

<TabItem value="graphql" label="GraphQL">
Query the model's training jobs:

```graphql
query {
  mlModel(
    urn:"urn:li:mlModel:(urn:li:dataPlatform:mlflow,arima_model,PROD)"
  ) {
    name
    properties {
      mlModelLineageInfo {
        trainingJobs
      }
    }
  }
}
```

View the relationship:

```json
{
  "data": {
    "mlModel": {
      "name": "arima_model",
      "properties": {
        "mlModelLineageInfo": {
          "trainingJobs": [
            "urn:li:dataProcessInstance:simple_training_run_test"
          ]
        }
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Add Run To Model Group

Create a direct connection between a run and a model group:

```python
client.add_run_to_model_group(model_group_urn=model_group_urn, run_urn=run_urn)
```

This connection lets you:
- View model groups in the run's lineage
- Query training jobs at the group level
- Track training history for model families

<Tabs>
<TabItem value="UI" label="UI">

See model groups in the **Run** page under the **Lineage** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-lineage-model-group.png"/>
</p>
<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-lineage-model-group-graph.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query the model group's training jobs:

```graphql
query {
  mlModelGroup(
    urn:"urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,airline_forecast_model_group,PROD)"
  ) {
    name
    properties {
      mlModelLineageInfo {
        trainingJobs
      }
    }
  }
}
```

Check the relationship:

```json
{
  "data": {
    "mlModelGroup": {
      "name": "airline_forecast_model_group",
      "properties": {
        "mlModelLineageInfo": {
          "trainingJobs": [
            "urn:li:dataProcessInstance:simple_training_run_test"
          ]
        }
      }
    }
  }
}
```

</TabItem>
</Tabs>

### Add Dataset To Run

Track input and output datasets for your training runs:

```python
client.add_input_datasets_to_run(
    run_urn=run_urn, 
    dataset_urns=[str(input_dataset_urn)]
)

client.add_output_datasets_to_run(
    run_urn=run_urn, 
    dataset_urns=[str(output_dataset_urn)]
)
```

These connections help you:
- Track data lineage
- Understand data dependencies
- Ensure reproducibility
- Monitor data quality impacts

Find dataset relationships in the **Lineage** tab of either the **Dataset** or **Run** page:
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/run-lineage-dataset-graph.png"/>
</p>

## Full Overview

Here's your complete ML system with all components connected:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/ml/lineage-full.png"/>
</p>

You now have a complete lineage view of your ML assets, from training data through runs to production models!

You can check out the full code for this tutorial [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/ai/dh_ai_client_sample.py).
## What's Next?

To see these integrations in action:
- Watch our [Townhall demo](https://youtu.be/_WUoVqkF2Zo?feature=shared&t=1932) showcasing the MLflow integration
- Explore our detailed documentation:
  - [MLflow Integration Guide](/docs/generated/ingestion/sources/mlflow.md)
  - [Amazon SageMaker Integration Guide](/docs/generated/ingestion/sources/sagemaker.md)