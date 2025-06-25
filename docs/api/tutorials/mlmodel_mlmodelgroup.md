import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MLModel & ML Model Group

## Why Would You Use MLModel and MLModelGroup?

MLModel and MLModelGroup entities are used to represent machine learning models and their associated groups within a metadata ecosystem. They allow users to define, manage, and monitor machine learning models, including their versions, configurations, and performance metrics.

### Goal Of This Guide

This guide will show you how to

- Create: create a dataset with three columns.
- Delete: delete a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Dataset

<Tabs>
<TabItem value="graphql" label="GraphQL">

> 🚫 Creating a dataset via `graphql` is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information.

</TabItem>
<TabItem value="java" label="Java">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetAdd.java show_path_as_comment }}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_schema.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Dataset

You can now see `realestate_db.sales` dataset has been created.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-created.png"/>
</p>
