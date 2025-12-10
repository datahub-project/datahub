---
title: MLModel & MLModelGroup
slug: /api/tutorials/mlmodel-mlmodelgroup
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/mlmodel-mlmodelgroup.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MLModel & MLModelGroup

## Why Would You Use MLModel and MLModelGroup?

MLModel and MLModelGroup entities are used to represent machine learning models and their associated groups within a metadata ecosystem. They allow users to define, manage, and monitor machine learning models, including their versions, configurations, and performance metrics.

### Goal Of This Guide

This guide will show you how to

- Create an MLModel or MLModelGroup.
- Associate an MLModel with an MLModelGroup.
- Read MLModel and MLModelGroup entities.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create MLModelGroup

You can create an MLModelGroup by providing the necessary attributes such as name, platform, and other metadata.

```python
# Inlined from /metadata-ingestion/examples/library/create_mlmodel_group.py
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodelgroup import MLModelGroup

client = DataHubClient.from_env()

mlmodel_group = MLModelGroup(
    id="my-recommendations-model-group",
    name="My Recommendations Model Group",
    platform="mlflow",
    description="Grouping of ml model related to home page recommendations",
    custom_properties={
        "framework": "pytorch",
    },
)

client.entities.update(mlmodel_group)

```

## Create MLModel

You can create an MLModel by providing the necessary attributes such as name, platform, and other metadata.

```python
# Inlined from /metadata-ingestion/examples/library/create_mlmodel.py
import datahub.metadata.schema_classes as models
from datahub.metadata.urns import MlFeatureUrn, MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

mlmodel = MLModel(
    id="my-recommendations-model",
    name="My Recommendations Model",
    platform="mlflow",
    model_group=MlModelGroupUrn(
        platform="mlflow", name="my-recommendations-model-group"
    ),
    custom_properties={
        "framework": "pytorch",
    },
    extra_aspects=[
        models.MLModelPropertiesClass(
            mlFeatures=[
                str(
                    MlFeatureUrn(
                        feature_namespace="users_feature_table", name="user_signup_date"
                    )
                ),
                str(
                    MlFeatureUrn(
                        feature_namespace="users_feature_table",
                        name="user_last_active_date",
                    )
                ),
            ]
        )
    ],
    training_metrics={
        "accuracy": "1.0",
        "precision": "0.95",
        "recall": "0.90",
        "f1_score": "0.92",
    },
    hyper_params={
        "learning_rate": "0.01",
        "num_epochs": "100",
        "batch_size": "32",
    },
)

client.entities.update(mlmodel)

```

Note that you can associate an MLModel with an MLModelGroup by providing the group URN when creating the MLModel.

You can also set MLModelGroup later by updating the MLModel entity as shown below.

```python
# Inlined from /metadata-ingestion/examples/library/add_mlgroup_to_mlmodel.py
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient
from datahub.sdk.mlmodel import MLModel

client = DataHubClient.from_env()

model = MLModel(
    id="my-recommendations-model",
    platform="mlflow",
)

model.set_model_group(
    MlModelGroupUrn(
        platform="mlflow",
        name="my-recommendations-model-group",
    )
)

client.entities.upsert(model)

```

## Read MLModelGroup

You can read an MLModelGroup by providing the group URN.

```python
# Inlined from /metadata-ingestion/examples/library/read_mlmodel_group.py
from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MlModelGroupUrn.from_string(...)
mlmodel_group_urn = MlModelGroupUrn(
    platform="mlflow", name="my-recommendations-model-group"
)

mlmodel_group_entity = client.entities.get(mlmodel_group_urn)
print("Model Group Name: ", mlmodel_group_entity.name)
print("Model Group Description: ", mlmodel_group_entity.description)
print("Model Group Custom Properties: ", mlmodel_group_entity.custom_properties)

```

#### Expected Output

```python
>> Model Group Name:  My Recommendations Model Group
>> Model Group Description:  A group for recommendations models
>> Model Group Custom Properties:  {'owner': 'John Doe', 'team': 'recommendations', 'domain': 'marketing'}
```

## Read MLModel

You can read an MLModel by providing the model URN.

```python
# Inlined from /metadata-ingestion/examples/library/read_mlmodel.py
from datahub.metadata.urns import MlModelUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use MlModelUrn.from_string(...)
mlmodel_urn = MlModelUrn(platform="mlflow", name="my-recommendations-model")

mlmodel_entity = client.entities.get(mlmodel_urn)
print("Model Name: ", mlmodel_entity.name)
print("Model Description: ", mlmodel_entity.description)
print("Model Group: ", mlmodel_entity.model_group)
print("Model Hyper Parameters: ", mlmodel_entity.hyper_params)

```

#### Expected Output

```python
>> Model Name:  My Recommendations Model
>> Model Description:  A model for recommending products to users
>> Model Group:  urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,my-recommendations-model,PROD)
>> Model Hyper Parameters:  [MLHyperParamClass({'name': 'learning_rate', 'description': None, 'value': '0.01', 'createdAt': None}), MLHyperParamClass({'name': 'num_epochs', 'description': None, 'value': '100', 'createdAt': None}), MLHyperParamClass({'name': 'batch_size', 'description': None, 'value': '32', 'createdAt': None})]
```
