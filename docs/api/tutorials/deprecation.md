


# Deprecation

## Why Would You Deprecate Datasets?

The Deprecation feature on DataHub indicates the status of an entity. For datasets, keeping the deprecation status up-to-date is important to inform users and downstream systems of changes to the dataset's availability or reliability. By updating the status, you can communicate changes proactively, prevent issues and ensure users are always using highly trusted data assets.

### Goal Of This Guide

This guide will show you how to read or update deprecation status of a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [DataHub Quickstart Guide](/docs/quickstart.md).

:::note
Before updating deprecation, you need to ensure the targeted dataset is already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.
:::

## Read Deprecation



#### GraphQL


```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    deprecation {
      deprecated
      decommissionTime
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "deprecation": {
        "deprecated": false,
        "decommissionTime": null
      }
    }
  },
  "extensions": {}
}
```




#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") { deprecation { deprecated decommissionTime } } }", "variables":{} }'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "deprecation": { "deprecated": false, "decommissionTime": null }
    }
  },
  "extensions": {}
}
```




#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_query_deprecation.py
from typing import Optional, Tuple

from datahub.metadata.schema_classes import DeprecationClass
from datahub.sdk import DataHubClient, DatasetUrn


def query_dataset_deprecation(
    client: DataHubClient, dataset_urn: DatasetUrn
) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Query the deprecation status of a dataset.

    Args:
        client: DataHub client to use for the query
        dataset_urn: URN of the dataset to check

    Returns:
        Tuple of (is_deprecated, deprecation_note, decommission_time_millis)
    """
    dataset = client.entities.get(dataset_urn)

    deprecation = dataset._get_aspect(DeprecationClass)
    if deprecation and deprecation.deprecated:
        return (True, deprecation.note, deprecation.decommissionTime)
    return (False, None, None)


def main(client: Optional[DataHubClient] = None) -> None:
    """
    Main function to query dataset deprecation example.

    Args:
        client: Optional DataHub client (for testing). If not provided, creates one from env.
    """
    client = client or DataHubClient.from_env()

    dataset_urn = DatasetUrn(platform="hive", name="fct_users_created", env="PROD")

    is_deprecated, note, decommission_time = query_dataset_deprecation(
        client, dataset_urn
    )

    if is_deprecated:
        print(f"Dataset is deprecated: {note}")
        if decommission_time:
            print(f"Decommission time: {decommission_time}")
    else:
        print("Dataset is not deprecated")


if __name__ == "__main__":
    main()

```




## Update Deprecation



#### GraphQL


```json
mutation updateDeprecation {
    updateDeprecation(input: { urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)", deprecated: true })
}
```

Also note that you can update deprecation status of multiple entities or subresource using `batchUpdateDeprecation`.

```json
mutation batchUpdateDeprecation {
    batchUpdateDeprecation(
      input: {
        deprecated: true,
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}

```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "updateDeprecation": true
  },
  "extensions": {}
}
```




#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDeprecation { updateDeprecation(input: { deprecated: true, urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "removeTag": true }, "extensions": {} }
```




#### Python





### Expected Outcomes of Updating Deprecation

You can now see the dataset `fct_users_created` has been marked as `Deprecated.`

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/deprecation-updated.png"/>
</p>
