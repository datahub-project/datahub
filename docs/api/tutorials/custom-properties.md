import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Custom Properties

## Why Would You Use Custom Properties on Datasets?

Custom properties to datasets can help to provide additional information about the data that is not readily available in the standard metadata fields. Custom properties can be used to describe specific attributes of the data, such as the units of measurement used, the date range covered, or the geographical region the data pertains to. This can be particularly helpful when working with large and complex datasets, where additional context can help to ensure that the data is being used correctly and effectively.

DataHub models custom properties of a Dataset as a map of key-value pairs of strings.

Custom properties can also be used to enable advanced search and discovery capabilities, by allowing users to filter and sort datasets based on specific attributes. This can help users to quickly find and access the data they need, without having to manually review large numbers of datasets.

### Goal Of This Guide

This guide will show you how to add, remove or replace custom properties on a dataset `fct_users_deleted`. Here's what each operation means:

- Add: Add custom properties to a dataset without affecting existing properties
- Remove: Removing specific properties from the dataset without affecting other properties
- Replace: Completely replacing the entire property map without affecting other fields that are located in the same aspect. e.g. `DatasetProperties` aspect contains `customProperties` as well as other fields like `name` and `description`.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before adding custom properties, you need to ensure the target dataset is already present in your DataHub instance.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

In this example, we will add some custom properties `cluster_name` and `retention_time` to the dataset `fct_users_deleted`.

After you have ingested sample data, the dataset `fct_users_deleted` should have a custom properties section with `encoding` set to `utf-8`.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-properties-before.png"/>
</p>


```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" --aspect datasetProperties
{
  "datasetProperties": {
    "customProperties": {
      "encoding": "utf-8"
    },
    "description": "table containing all the users deleted on a single day",
    "tags": []
  }
}
```

## Add Custom Properties programmatically

The following code adds custom properties `cluster_name` and `retention_time` to a dataset named `fct_users_deleted` without affecting existing properties.

<Tabs>
<TabItem value="graphql" label="GraphQL">

> 🚫 Adding Custom Properties on Dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information,

</TabItem>
<TabItem value="java" label="Java">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAdd.java show_path_as_comment }}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_custom_properties_patch.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Adding Custom Properties

You can now see the two new properties are added to `fct_users_deleted` and the previous property `encoding` is unchanged.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-properties-added.png"/>
</p>


We can also verify this operation by programmatically checking the `datasetProperties` aspect after running this code using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" --aspect datasetProperties
{
  "datasetProperties": {
    "customProperties": {
      "encoding": "utf-8",
      "cluster_name": "datahubproject.acryl.io",
      "retention_time": "2 years"
    },
    "description": "table containing all the users deleted on a single day",
    "tags": []
  }
}
```

## Add and Remove Custom Properties programmatically

The following code shows you how can add and remove custom properties in the same call. In the following code, we add custom property `cluster_name` and remove property `retention_time` from a dataset named `fct_users_deleted` without affecting existing properties.

<Tabs>
<TabItem value="graphql" label="GraphQL">

> 🚫 Adding and Removing Custom Properties on Dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information,

</TabItem>
<TabItem value="java" label="Java">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesAddRemove.java show_path_as_comment }}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_add_remove_custom_properties_patch.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Add and Remove Operations on Custom Properties

You can now see the `cluster_name` property is added to `fct_users_deleted` and the `retention_time` property is removed.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-properties-added-removed.png"/>
</p>


We can also verify this operation programmatically by checking the `datasetProperties` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" --aspect datasetProperties
{
  "datasetProperties": {
    "customProperties": {
      "encoding": "utf-8",
      "cluster_name": "datahubproject.acryl.io"
    },
    "description": "table containing all the users deleted on a single day",
    "tags": []
  }
}
```

## Replace Custom Properties programmatically

The following code replaces the current custom properties with a new properties map that includes only the properties `cluster_name` and `retention_time`. After running this code, the previous `encoding` property will be removed.

<Tabs>
<TabItem value="graphql" label="GraphQL">

> 🚫 Replacing Custom Properties on Dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information,

</TabItem>
<TabItem value="java" label="Java">

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesReplace.java show_path_as_comment }}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/dataset_replace_properties.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome of Replacing Custom Properties

You can now see the `cluster_name` and `retention_time` properties are added to `fct_users_deleted` but the previous `encoding` property is no longer present.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-properties-replaced.png"/>
</p>


We can also verify this operation programmatically by checking the `datasetProperties` aspect using the `datahub` cli.

```shell
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)" --aspect datasetProperties
{
  "datasetProperties": {
    "customProperties": {
      "cluster_name": "datahubproject.acryl.io",
      "retention_time": "2 years"
    },
    "description": "table containing all the users deleted on a single day",
    "tags": []
  }
}
```
