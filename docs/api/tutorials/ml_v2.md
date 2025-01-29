import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ML System with DataHub

## Why Would You Integrate ML System with DataHub?

As a data practitioner, keeping track of your ML experiments, models, and their relationships can be challenging. DataHub makes this easier by providing a central place to organize and track your ML assets. 

This guide will show you how to integrate your ML workflows with DataHub. 
With this integration, you can easily find and share ML models across your organization. 
Your team can track how models evolve over time and understand how training data connects to each model. 
Most importantly, it enables seamless collaboration on ML projects by making everything discoverable and connected.

## Goals Of This Guide

In this guide, you'll learn how to:
- Create your basic ML components (models, experiments, runs)
- Connect these components to build a complete ML system
- Track relationships between models, data, and experiments

## Core ML Concepts

Here's what you need to know about the key components, based on MLflow's terminology:

- **Experiments** are collections of **training runs** for the same project, like all attempts to build a churn predictor.
- **Training Runs** are attempts to train a **model** within an **experiment**, capturing your parameters and results.
- **Model** organize related model versions together, like all versions of your churn predictor.
- **Model Versions** are successful training runs that you've registered for production use.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/concept-diagram.png"/>
</p>

The hierarchy works like this:
1. Every run belongs to an experiment
2. Successful runs can become model versions
3. Model versions belong to a model group
4. Not every run becomes a model version

:::note Terminology
Here's how DataHub and MLflow terms map to each other. 
For more details, see the [MLflow integration doc](/docs/generated/ingestion/sources/mlflow.md).:

| DataHub | MLflow | Description |
|---------|---------|-------------|
| ML Model Group | Model | Collection of related model versions |
| ML Model | Model Version | Specific version of a trained model |
| ML Training Run | Run | Single training attempt |
| ML Experiment | Experiment | Project workspace |
:::

## Basic Setup

For this tutorial, you need to deploy DataHub Quickstart locally.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

Next, you need to set up the Python client for DataHub.
Create a token in DataHub UI and replace `<your_token>` with your token in the code below:

```python
from mlflow_dh_client import MLflowDatahubClient

client = MLflowDatahubClient(token="<your_token>")
```

:::note Verifying via GraphQL
In this Guide, we'll show you how to verify your changes using GraphQL queries.
You can run these queries in the DataHub UI -- just go to `https://localhost:9002/api/graphiql` and paste the query.
:::
## Create Simple ML Entities

In this section, we'll create the basic building blocks of your ML system. These components will help you organize your ML work and make it discoverable by your team.

### Create Model Group

A model group is like a folder that contains different versions of a similar model. For example, all versions of your "Customer Churn Predictor" would go in one group.

<Tabs>
<TabItem value="simple" label="Simple Version">
Here's how to create a basic model group with just an identifier:

```python
client.create_model_group(
    group_id="airline_forecast_models_group",
)
```

</TabItem>
<TabItem value="detailed" label="Detailed Version">

For production use, you can add rich metadata like descriptions, creation timestamps, and team information:


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
You can see your new model group in the DataHub UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/model-group-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
You can also query your model group using GraphQL to check its properties:

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

The response should show your model group's details:

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

### Create Model

Now let's create a specific model version. This represents a trained model that you might deploy.

<Tabs>
<TabItem value="simple" label="Simple Version">

Here's the minimum needed to create a model (note that version is required):


```python
client.create_model(
    model_id="arima_model",
    version="1.0",
)
```

</TabItem>
<TabItem value="detailed" label="Detailed Version">

For a production model, you'll want to include metrics, parameters, and other metadata:


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
You can view your model's details in the DataHub UI.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/model-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Here's how to query your model's information using GraphQL:

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

You should see details about your model:

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

### Create Experiment

An experiment helps you organize multiple training runs for a specific project.

<Tabs>
<TabItem value="simple" label="Simple Version">
Create a basic experiment with just an ID:

```python
client.create_experiment(
    experiment_id="airline_forecast_experiment",
)
```

</TabItem>
<TabItem value="detailed" label="Detailed Version">
Add more context to your experiment with metadata:

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
View your experiment's details in the UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/experiment-empty.png"/>
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

Check the experiment's details in the response:

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

### Create Training Run

A training run captures everything about a specific model training attempt.

<Tabs>
<TabItem value="simple" label="Simple Version">

Create a basic training run:

```python
client.create_training_run(
    run_id="simple_training_run_4",
)
```

</TabItem>
<TabItem value="detailed" label="Detailed Version">

For a production run, you'll want to include metrics, parameters, and other metadata:

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

Check your training run:

<Tabs>
<TabItem value="UI" label="UI">

View the training run details in DataHub UI:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-empty.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">
Query your training run information:

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

See the run details in the response:

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

Now comes the important part - connecting all these components together. By establishing relationships between your ML assets, you'll be able to:
- Track model lineage (which data and runs produced which models)
- Monitor model evolution over time
- Understand dependencies between different components
- Enable comprehensive searching and filtering

### Add Model To Model Group

Connect your model to its group to organize related model versions together:

```python
client.add_model_to_model_group(model_urn=model_urn, group_urn=model_group_urn)
```

After connecting your model to a group, you'll be able to:
- View all versions of a model in one place
- Track model evolution over time
- Compare different versions easily
- Manage model lifecycles better

<Tabs>
<TabItem value="UI" label="UI">

In **Model Group** view, you can see the model versions under the **Models** section:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/model-group-with-model.png"/>
</p>

In **Model** page , you can see the group it belongs to under the **Group** tab:
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/model-with-model-group.png"/>
</p>

</TabItem>

<TabItem value="graphql" label="GraphQL">

You can query the model's group using `groups` field to see the relationship:

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

Expected response:

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

Organize your training runs by adding them to an experiment:

```python
client.add_run_to_experiment(run_urn=run_urn, experiment_urn=experiment_urn)
```

This connection enables you to:
- Group related training attempts together
- Compare runs within the same experiment
- Track progress toward your ML goals
- Share results with your team

<Tabs>
<TabItem value="UI" label="UI">

Under **Entities** tab in the **Experiment** page, you can see the runs associated with the experiment:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/experiment-with-run.png"/>
</p>

In the **Run** page, you can see the experiment it belongs to.

<p align="center">
  <img width="40%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-with-experiment.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">

Query the run's experiment through `parentContainers`:

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

See the relationship details:

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

Link a training run to the model it produced:

```python
client.add_run_to_model(model_urn=model_urn, run_urn=run_urn)
```

This connection helps you:
- Track which training runs produced which models
- Understand model provenance
- Debug model issues by examining training history
- Monitor model evolution

<Tabs>
<TabItem value="UI" label="UI">

In the **Model** page, you can see the runs that produced the model as **Source Run** under the **Summary** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/model-with-source-run.png"/>
</p>

In the **Run** page, you can see the related model under the **Lineage** tab:

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-lineage-model.png"/>
</p>
<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-lineage-model-graph.png"/>
</p>

</TabItem>

<TabItem value="graphql" label="GraphQL">

You can query the model's training jobs using `trainingJobs` to see the relationship:

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

Check the relationship in the response:

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

Connect a training run directly to a model group:

```python
client.add_run_to_model_group(model_group_urn=model_group_urn, run_urn=run_urn)
```

After establishing this connection, you'll be able to:
- View model groups in the run's lineage tab
- Query training jobs at the group level

<Tabs>
<TabItem value="UI" label="UI">

In the **Run** page, you can see the model groups associated with the group under the **Lineage** tab:
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-lineage-model-group.png"/>
</p>
<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-lineage-model-group-graph.png"/>
</p>
</TabItem>

<TabItem value="graphql" label="GraphQL">

You can query the model groups's training jobs using `trainingJobs` to see the relationship:


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

Verify the relationship:

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

Track the data used in your training runs:

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

This connection enables you to:
- Track data lineage for your models
- Understand data dependencies
- Ensure reproducibility of your training runs
- Monitor data quality impacts on model performance

You can verify the relationship in the **Lineage** Tab either in **DataSet** page or **Run** page.
<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/run-lineage-dataset-graph.png"/>
</p>

## Full Overview

This is how your ML system looks after connecting all the components.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/add-img-for-ml/imgs/apis/tutorials/ml/lineage-full.png"/>
</p>

Now you have a complete lineage view of your ML assets -- from training runs to models to datasets.

You can check the complete script [here]().

## What's Next?

To see this integration in action and learn about real-world use cases:
- Watch our [Townhall demo](https://youtu.be/_WUoVqkF2Zo?feature=shared&t=1932) on MLflow integration with DataHub
- Check out the discussion in our [Slack community](https://slack.datahubproject.io)
- Readh our [MLflow integration doc](/docs/generated/ingestion/sources/mlflow.md) for more details