# Adding or Replacing Custom Properties on Datasets

## Why Would You Add or Replace Custom Properties to Datasets? 
Adding custom properties to datasets can help to provide additional information about the data that is not readily available in the standard metadata fields. Custom properties can be used to describe specific attributes of the data, such as the units of measurement used, the date range covered, or the geographical region the data pertains to. This can be particularly helpful when working with large and complex datasets, where additional context can help to ensure that the data is being used correctly and effectively.

DataHub models custom properties of a Dataset as a map of key-value pairs of strings.

Custom properties can also be used to enable advanced search and discovery capabilities, by allowing users to filter and sort datasets based on specific attributes. This can help users to quickly find and access the data they need, without having to manually review large numbers of datasets.

### When to Add or Replace

There may be situations where it is more appropriate to replace existing custom properties on a dataset, rather than simply adding new ones.

Adding and removing specific properties is supported through patch semantics while full replaces can be done using the standard upsert APIs.


### Goal Of This Guide
This guide will show you how to add or replace custom properties on dataset `fct_users_deleted`.


## Prerequisites
For this tutorial, you need to deploy DataHub Quickstart and ingest sample data. 
For detailed steps, please refer to [Prepare Local DataHub Environment](/docs/api/tutorials/references/prepare-datahub.md).

:::note
Before adding custom properties, you need to ensure the target dataset is already present in your DataHub instance. 
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

In this example, we will add some custom properties `cluster_name` and `retention_time` to the dataset `fct_users_deleted`.

## Add Custom Properties With Java SDK
The following code adds custom properties `cluster_name` and `retention_time` to a dataset named `fct_users_deleted` without affecting existing properties.

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomProperties.java }}
```

## Replace Custom Properties With Java SDK
The following code replaces the current custom properties to a dataset named `fct_users_deleted` with `cluster_name` and `retention_time`. Note, since this
is utilizing the upsert API this will do a full replace of the Dataset Properties aspect and will not retain other fields unless specified. To just modify custom properties
you can use the above example and combine it with delete requests for unwanted properties.

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetCustomPropertiesReplace.java }}
```

## Add Custom Properties With Python SDK
> 🚫 Adding Custom Properties on Dataset via the Python SDK using patch semantics is currently not supported.

## Replace Custom Properties With Python SDK
Replacing the custom properties can be done in a similar way to [dataset_add_documentation.py](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/dataset_add_documentation.py)
just modify description with a custom properties map.
 

> ## Add Custom Properties With GraphQL (Not Supported)
> 🚫 Adding Custom Properties on Dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information.

## Replace Custom Properties With GraphQL (Not Supported)
> 🚫 Replacing Custom Properties on Dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information.

## Expected Outcomes
The custom properties are added to `fct_users_deleted` and you are able to search by these properties.
To do so you can type `/q customProperties:cluster_name=datahubproject.acryl.io` into the search bar in the UI.

