import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Feature Store Integration With DataHub

## Why Would You Integrate Feature Store with DataHub?

Feature Store is a data management layer that stores, organizes, and manages features for machine learning models. It is a centralized repository for features that can be used across different AI/ML models. 
By integrating Feature Store with DataHub, you can track the lineage of features used in AI/ML models, understand how features are generated, and how they are used to train models.

For technical details on feature store entities, please refer to the following docs:

- [MlFeature](/docs/generated/metamodel/entities/mlFeature.md)
- [MlPrimaryKey](/docs/generated/metamodel/entities/mlPrimaryKey.md)
- [MlFeatureTable](/docs/generated/metamodel/entities/mlFeatureTable.md)

### Goal Of This Guide

This guide will show you how to

- Create feature store entities: MlFeature, MlFeatureTable, MlPrimaryKey
- Read feature store entities: MlFeature, MlFeatureTable, MlPrimaryKey
- Attach MlModel to MlFeature
- Attach MlFeatures to MlFeatureTable
- Attached MlFeatures to upstream Datasets that power them

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create ML Entities

:::note 
For creating MLModels and MLGroups, please refer to [AI/ML Integration Guide](/docs/api/tutorials/ml.md).
:::

### Create MlFeature

An ML Feature represents an instance of a feature that can be used across different machine learning models. Features are organized into Feature Tables to be consumed by machine learning models. For example, if we were modeling features for a Users Feature Table, the Features would be `age`, `sign_up_date`, `active_in_past_30_days` and so forth.Using Features in DataHub allows users to see the sources a feature was generated from and how a feature is used to train models.

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_mlfeature.py show_path_as_comment }}
```

Note that when creating a feature, you create upstream lineage to the data warehouse using `sources`.

</TabItem>
</Tabs>

### Create MlPrimaryKey

An ML Primary Key represents a specific element of a Feature Table that indicates what group the other features belong to. For example, if a Feature Table contained features for Users, the ML Primary Key would likely be `user_id` or some similar unique identifier for a user. Using ML Primary Keys in DataHub allow users to indicate how ML Feature Tables are structured.

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_mlprimarykey.py show_path_as_comment }}
```

Note that when creating a primary key, you create upstream lineage to the data warehouse using `sources`.

</TabItem>
</Tabs>

### Create MlFeatureTable

A feature table represents a group of similar Features that can all be used together to train a model. For example, if there was a Users Feature Table, it would contain documentation around how to use the Users collection of Features and references to each Feature and ML Primary Key contained within it.

<Tabs>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_mlfeature_table.py show_path_as_comment }}
```

Note that when creating a feature table, you connect the table to its features and primary key using `mlFeatures` and `mlPrimaryKeys`.

</TabItem>
</Tabs>


### Expected Outcome of creating entities

You can search the entities in DataHub UI.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/feature-table-created.png"/>
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

### Read MlPrimaryKey

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  mlPrimaryKey(urn: "urn:li:mlPrimaryKey:(user_features,user_id)"){
    name
    featureNamespace
    description
    dataType
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
    "mlPrimaryKey": {
      "name": "user_id",
      "featureNamespace": "user_features",
      "description": "User's internal ID",
      "dataType": "ORDINAL",
      "properties": {
        "description": "User's internal ID",
        "dataType": "ORDINAL",
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
    "query": "query {  mlPrimaryKey(urn: \"urn:li:mlPrimaryKey:(user_features,user_id)\"){    name    featureNamespace    description    dataType    properties {      description      dataType      version {        versionTag      }    }  }}"
}'
```

Expected response:

```json
{
  "data": {
    "mlPrimaryKey": {
      "name": "user_id",
      "featureNamespace": "user_features",
      "description": "User's internal ID",
      "dataType": "ORDINAL",
      "properties": {
        "description": "User's internal ID",
        "dataType": "ORDINAL",
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
{{ inline /metadata-ingestion/examples/library/read_mlprimarykey.py show_path_as_comment }}
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
          ...{
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
          ...{
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
