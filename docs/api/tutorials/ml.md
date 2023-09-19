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
{{ inline /metadata-ingestion/examples/library/create_mlfeature.py show_path_as_comment }}
```

Note that when creating a feature, you can access a list of data sources using `sources`.

</TabItem>
</Tabs>

### Create MlFeatureTable

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_mlfeature_table.py show_path_as_comment }}
```

Note that when creating a feature table, you can access a list of features using `mlFeatures`.

</TabItem>
</Tabs>

### Create MlModel

Please note that an MlModel represents the outcome of a single training run for a model, not the collective results of all model runs.

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_mlmodel.py show_path_as_comment }}
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
{{ inline /metadata-ingestion/examples/library/create_mlmodel_group.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of creating entities

You can search the entities in DataHub UI.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/feature-table-created.png"/>
</p>



<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/model-group-created.png"/>
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
{{ inline /metadata-ingestion/examples/library/read_mlfeature.py show_path_as_comment }}
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
{{ inline /metadata-ingestion/examples/library/read_mlfeature_table.py show_path_as_comment }}
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
{{ inline /metadata-ingestion/examples/library/read_mlmodel.py show_path_as_comment }}
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
{{ inline /metadata-ingestion/examples/library/read_mlmodel_group.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Add ML Entities

### Add MlFeature to MlFeatureTable

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/add_mlfeature_to_mlfeature_table.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Add MlFeature to MLModel

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/add_mlfeature_to_mlmodel.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Add MLGroup To MLModel

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/add_mlgroup_to_mlmodel.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Adding ML Entities

You can access to `Features` or `Group` Tab of each entity to view the added entities.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/feature-added-to-model.png"/>
</p>



<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/model-group-added-to-model.png"/>
</p>

