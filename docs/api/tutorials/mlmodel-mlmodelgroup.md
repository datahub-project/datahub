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
{{ inline /metadata-ingestion/examples/library/create_mlmodel_group.py show_path_as_comment }}
```

## Create MLModel

You can create an MLModel by providing the necessary attributes such as name, platform, and other metadata.

```python
{{ inline /metadata-ingestion/examples/library/create_mlmodel.py show_path_as_comment }}
```

Note that you can associate an MLModel with an MLModelGroup by providing the group URN when creating the MLModel.

You can also set MLModelGroup later by updating the MLModel entity as shown below.

```python
{{ inline /metadata-ingestion/examples/library/add_mlgroup_to_mlmodel.py show_path_as_comment }}
```

## Read MLModelGroup

You can read an MLModelGroup by providing the group URN.

```python
{{ inline /metadata-ingestion/examples/library/read_mlmodel_group.py show_path_as_comment }}
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
{{ inline /metadata-ingestion/examples/library/read_mlmodel.py show_path_as_comment }}
```

#### Expected Output

```python
>> Model Name:  My Recommendations Model
>> Model Description:  A model for recommending products to users
>> Model Group:  urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,my-recommendations-model,PROD)
>> Model Hyper Parameters:  [MLHyperParamClass({'name': 'learning_rate', 'description': None, 'value': '0.01', 'createdAt': None}), MLHyperParamClass({'name': 'num_epochs', 'description': None, 'value': '100', 'createdAt': None}), MLHyperParamClass({'name': 'batch_size', 'description': None, 'value': '32', 'createdAt': None})]
```
