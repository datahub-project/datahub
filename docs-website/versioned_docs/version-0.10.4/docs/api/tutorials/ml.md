---
title: ML System
slug: /api/tutorials/ml
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/ml.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ML System

## Why Would You Integrate ML System with DataHub?

Machine learning systems have become a crucial feature in modern data stacks.
However, the relationships between the different components of a machine learning system, such as features, models, and feature tables, can be complex.
Thus, it is essential for these systems to be discoverable to facilitate easy access and utilization by other members of the organization.

For more information on ML entities, please refer to the following docs:

- [MlFeature](/docs/generated/metamodel/entities/mlFeature.md)
- [MlFeatureTable](/docs/generated/metamodel/entities/mlFeatureTable.md)
- [MlModel](/docs/generated/metamodel/entities/mlModel.md)
- [MlModelGroup](/docs/generated/metamodel/entities/mlModelGroup.md)

### Goal Of This Guide

This guide will show you how to

- Create ML entities: MlFeature, MlFeatureTable, MlModel, MlModelGroup
- Read ML entities: MlFeature, MlFeatureTable, MlModel, MlModelGroup
- Attach MlFeatureTable or MlModel to MlFeature

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create ML Entities

### Create MlFeature

<Tabs>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/create_mlfeature.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

dataset_urn = builder.make_dataset_urn(
    name="fct_users_deleted", platform="hive", env="PROD"
)
feature_urn = builder.make_ml_feature_urn(
    feature_table_name="my-feature-table",
    feature_name="my-feature",
)

#  Create feature
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeature",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_urn,
    aspectName="mlFeatureProperties",
    aspect=models.MLFeaturePropertiesClass(
        description="my feature", sources=[dataset_urn], dataType="TEXT"
    ),
)

# Emit metadata!
emitter.emit(metadata_change_proposal)

```

Note that when creating a feature, you can access a list of data sources using `sources`.

</TabItem>
</Tabs>

### Create MlFeatureTable

<Tabs>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/create_mlfeature_table.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="my-feature-table", platform="feast"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature", feature_table_name="my-feature-table"
    ),
    builder.make_ml_feature_urn(
        feature_name="my-feature2", feature_table_name="my-feature-table"
    ),
]
feature_table_properties = models.MLFeatureTablePropertiesClass(
    description="Test description", mlFeatures=feature_urns
)

# MCP creation
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeatureTable",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_table_urn,
    aspect=feature_table_properties,
)

# Emit metadata!
emitter.emit(metadata_change_proposal)

```

Note that when creating a feature table, you can access a list of features using `mlFeatures`.

</TabItem>
</Tabs>

### Create MlModel

Please note that an MlModel represents the outcome of a single training run for a model, not the collective results of all model runs.

<Tabs>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/create_mlmodel.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})
model_urn = builder.make_ml_model_urn(
    model_name="my-test-model", platform="science", env="PROD"
)
model_group_urns = [
    builder.make_ml_model_group_urn(
        group_name="my-model-group", platform="science", env="PROD"
    )
]
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature", feature_table_name="my-feature-table"
    ),
    builder.make_ml_feature_urn(
        feature_name="my-feature2", feature_table_name="my-feature-table"
    ),
]

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModel",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_urn,
    aspectName="mlModelProperties",
    aspect=models.MLModelPropertiesClass(
        description="my feature",
        groups=model_group_urns,
        mlFeatures=feature_urns,
        trainingMetrics=[
            models.MLMetricClass(
                name="accuracy", description="accuracy of the model", value="1.0"
            )
        ],
        hyperParams=[
            models.MLHyperParamClass(
                name="hyper_1", description="hyper_1", value="0.102"
            )
        ],
    ),
)

# Emit metadata!
emitter.emit(metadata_change_proposal)

```

Note that when creating a model, you can access a list of features using `mlFeatures`.
Additionally, you can access the relationship to model groups with `groups`.

</TabItem>
</Tabs>

### Create MlModelGroup

Please note that an MlModelGroup serves as a container for all the runs of a single ML model.

<Tabs>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/create_mlmodel_group.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})
model_group_urn = builder.make_ml_model_group_urn(
    group_name="my-model-group", platform="science", env="PROD"
)


metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModelGroup",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_group_urn,
    aspectName="mlModelGroupProperties",
    aspect=models.MLModelGroupPropertiesClass(
        description="my model group",
    ),
)


# Emit metadata!
emitter.emit(metadata_change_proposal)

```

</TabItem>
</Tabs>

### Expected Outcome of creating entities

You can search the entities in DataHub UI.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/feature-table-created.png"/>
</p>

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/model-group-created.png"/>
</p>

## Read ML Entities

### Read MLFeature

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  mlFeature(urn: "urn:li:mlFeature:(test_feature_table_all_feature_dtypes,test_BOOL_LIST_feature)"){
    name
    featureNamespace
    description
    properties {
      description
      dataType
      version {
        versionTag
      }
    }
  }
}
```

Expected response:

```json
{
  "data": {
    "mlFeature": {
      "name": "test_BOOL_LIST_feature",
      "featureNamespace": "test_feature_table_all_feature_dtypes",
      "description": null,
      "properties": {
        "description": null,
        "dataType": "SEQUENCE",
        "version": null
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl" default>

```json
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "{ mlFeature(urn: \"urn:li:mlFeature:(test_feature_table_all_feature_dtypes,test_BOOL_LIST_feature)\") { name featureNamespace description properties { description dataType version { versionTag } } } }"
}'
```

Expected response:

```json
{
  "data": {
    "mlFeature": {
      "name": "test_BOOL_LIST_feature",
      "featureNamespace": "test_feature_table_all_feature_dtypes",
      "description": null,
      "properties": {
        "description": null,
        "dataType": "SEQUENCE",
        "version": null
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/read_mlfeature.py
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLFeaturePropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlFeature:(test_feature_table_all_feature_dtypes,test_BOOL_feature)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLFeaturePropertiesClass)

print(result)

```

</TabItem>
</Tabs>

### Read MLFeatureTable

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  mlFeatureTable(urn: "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,test_feature_table_all_feature_dtypes)"){
    name
    description
    platform {
      name
    }
    properties {
      description
      mlFeatures {
        name
      }
    }
  }
}
```

Expected Response:

```json
{
  "data": {
    "mlFeatureTable": {
      "name": "test_feature_table_all_feature_dtypes",
      "description": null,
      "platform": {
        "name": "feast"
      },
      "properties": {
        "description": null,
        "mlFeatures": [
          {
            "name": "test_BOOL_LIST_feature"
          },
          ...
          {
            "name": "test_STRING_feature"
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```json
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "{ mlFeatureTable(urn: \"urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,test_feature_table_all_feature_dtypes)\") { name description platform { name } properties { description mlFeatures { name } } } }"
}'
```

Expected Response:

```json
{
  "data": {
    "mlFeatureTable": {
      "name": "test_feature_table_all_feature_dtypes",
      "description": null,
      "platform": {
        "name": "feast"
      },
      "properties": {
        "description": null,
        "mlFeatures": [
          {
            "name": "test_BOOL_LIST_feature"
          },
          ...
          {
            "name": "test_STRING_feature"
          }
        ]
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/read_mlfeature_table.py
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLFeatureTablePropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,test_feature_table_all_feature_dtypes)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLFeatureTablePropertiesClass)

print(result)

```

</TabItem>
</Tabs>

### Read MLModel

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  mlModel(urn: "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)"){
    name
    description
    properties {
      description
      version
      type
      mlFeatures
      groups {
        urn
        name
      }
    }
  }
}
```

Expected Response:

```json
{
  "data": {
    "mlModel": {
      "name": "scienceModel",
      "description": "A sample model for predicting some outcome.",
      "properties": {
        "description": "A sample model for predicting some outcome.",
        "version": null,
        "type": "Naive Bayes classifier",
        "mlFeatures": null,
        "groups": []
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl" default>

```json
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "{ mlModel(urn: \"urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)\") { name description properties { description version type mlFeatures groups { urn name } } } }"
}'
```

Expected Response:

```json
{
  "data": {
    "mlModel": {
      "name": "scienceModel",
      "description": "A sample model for predicting some outcome.",
      "properties": {
        "description": "A sample model for predicting some outcome.",
        "version": null,
        "type": "Naive Bayes classifier",
        "mlFeatures": null,
        "groups": []
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/read_mlmodel.py
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLModelPropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLModelPropertiesClass)

print(result)

```

</TabItem>
</Tabs>

### Read MLModelGroup

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  mlModelGroup(urn: "urn:li:mlModelGroup:(urn:li:dataPlatform:science,my-model-group,PROD)"){
    name
    description
    platform {
      name
    }
    properties {
      description
    }
  }
}
```

Expected Response: (Note that this entity does not exist in the sample ingestion and you might want to create this entity first.)

```json
{
  "data": {
    "mlModelGroup": {
      "name": "my-model-group",
      "description": "my model group",
      "platform": {
        "name": "science"
      },
      "properties": {
        "description": "my model group"
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```json
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "{ mlModelGroup(urn: \"urn:li:mlModelGroup:(urn:li:dataPlatform:science,my-model-group,PROD)\") { name description platform { name } properties { description } } }"
}'
```

Expected Response: (Note that this entity does not exist in the sample ingestion and you might want to create this entity first.)

```json
{
  "data": {
    "mlModelGroup": {
      "name": "my-model-group",
      "description": "my model group",
      "platform": {
        "name": "science"
      },
      "properties": {
        "description": "my model group"
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/read_mlmodel_group.py
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import MLModelGroupPropertiesClass

# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

urn = "urn:li:mlModelGroup:(urn:li:dataPlatform:science,my-model-group,PROD)"
result = graph.get_aspect(entity_urn=urn, aspect_type=MLModelGroupPropertiesClass)

print(result)

```

</TabItem>
</Tabs>

## Add ML Entities

### Add MlFeature to MlFeatureTable

<Tabs>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/add_mlfeature_to_mlfeature_table.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import MLFeatureTablePropertiesClass

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="my-feature-table", platform="feast"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature2", feature_table_name="my-feature-table"
    ),
]

# This code concatenates the new features with the existing features in the feature table.
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
feature_table_properties = graph.get_aspect(
    entity_urn=feature_table_urn, aspect_type=MLFeatureTablePropertiesClass
)
if feature_table_properties:
    current_features = feature_table_properties.mlFeatures
    print("current_features:", current_features)
    if current_features:
        feature_urns += current_features

feature_table_properties = models.MLFeatureTablePropertiesClass(mlFeatures=feature_urns)
# MCP createion
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeatureTable",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_table_urn,
    aspect=feature_table_properties,
)

# Emit metadata! This is a blocking call
emitter.emit(metadata_change_proposal)

```

</TabItem>
</Tabs>

### Add MlFeature to MLModel

<Tabs>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/add_mlfeature_to_mlmodel.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import MLModelPropertiesClass

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

model_urn = builder.make_ml_model_urn(
    model_name="my-test-model", platform="science", env="PROD"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature3", feature_table_name="my-feature-table"
    ),
]

# This code concatenates the new features with the existing features in the model
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
model_properties = graph.get_aspect(
    entity_urn=model_urn, aspect_type=MLModelPropertiesClass
)
if model_properties:
    current_features = model_properties.mlFeatures
    print("current_features:", current_features)
    if current_features:
        feature_urns += current_features

model_properties = models.MLModelPropertiesClass(mlFeatures=feature_urns)

# MCP creation
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModel",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_urn,
    aspect=model_properties,
)

# Emit metadata!
emitter.emit(metadata_change_proposal)

```

</TabItem>
</Tabs>

### Add MLGroup To MLModel

<Tabs>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/add_mlgroup_to_mlmodel.py
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

model_group_urns = [
    builder.make_ml_model_group_urn(
        group_name="my-model-group", platform="science", env="PROD"
    )
]
model_urn = builder.make_ml_model_urn(
    model_name="science-model", platform="science", env="PROD"
)

# This code concatenates the new features with the existing features in the feature table.
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

target_model_properties = graph.get_aspect(
    entity_urn=model_urn, aspect_type=models.MLModelPropertiesClass
)
if target_model_properties:
    current_model_groups = target_model_properties.groups
    print("current_model_groups:", current_model_groups)
    if current_model_groups:
        model_group_urns += current_model_groups

model_properties = models.MLModelPropertiesClass(groups=model_group_urns)
# MCP createion
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModel",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_urn,
    aspect=model_properties,
)

# Emit metadata! This is a blocking call
emitter.emit(metadata_change_proposal)

```

</TabItem>
</Tabs>

### Expected Outcome of Adding ML Entities

You can access to `Features` or `Group` Tab of each entity to view the added entities.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/feature-added-to-model.png"/>
</p>

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/apis/tutorials/model-group-added-to-model.png"/>
</p>
