


# Terms

## Why Would You Use Terms on Datasets?

The Business Glossary(Term) feature in DataHub helps you use a shared vocabulary within the orgarnization, by providing a framework for defining a standardized set of data concepts and then associating them with the physical assets that exist within your data ecosystem.

For more information about terms, refer to [About DataHub Business Glossary](/docs/glossary/business-glossary.md).

### Goal Of This Guide

This guide will show you how to

- Create: create a term.
- Read : read terms attached to a dataset.
- Add: add a term to a column of a dataset or a dataset itself.
- Remove: remove a term from a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [DataHub Quickstart Guide](/docs/quickstart.md).

:::note
Before modifying terms, you need to ensure the target dataset is already present in your DataHub instance.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

For more information on how to set up for GraphQL, please refer to [How To Set Up GraphQL](/docs/api/graphql/how-to-set-up-graphql.md).

## Create Terms

The following code creates a term `Rate of Return`.



#### GraphQL


```json
mutation createGlossaryTerm {
  createGlossaryTerm(input: {
    name: "Rate of Return",
    id: "rateofreturn",
    description: "A rate of return (RoR) is the net gain or loss of an investment over a specified time period."
  },
  )
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "createGlossaryTerm": "urn:li:glossaryTerm:rateofreturn"
  },
  "extensions": {}
}
```




#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation createGlossaryTerm { createGlossaryTerm(input: { name: \"Rate of Return\", id:\"rateofreturn\", description: \"A rate of return (RoR) is the net gain or loss of an investment over a specified time period.\" }) }", "variables":{}}'
```

Expected Response:

```json
{
  "data": { "createGlossaryTerm": "urn:li:glossaryTerm:rateofreturn" },
  "extensions": {}
}
```




#### Python


```python
# Inlined from /metadata-ingestion/examples/library/glossary_term_create_simple.py
from datahub.sdk import DataHubClient
from datahub.sdk.glossary_term import GlossaryTerm

client = DataHubClient.from_env()

term = GlossaryTerm(
    id="e5f67890",
    display_name="Rate of Return",
    definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
)

client.entities.upsert(term)

```




### Expected Outcome of Creating Terms

You can now see the new term `Rate of Return` has been created.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/term-created.png"/>
</p>

We can also verify this operation by programmatically searching `Rate of Return` term after running this code using the `datahub` cli.

```shell
datahub get --urn "urn:li:glossaryTerm:rateofreturn" --aspect glossaryTermInfo

{
  "glossaryTermInfo": {
    "definition": "A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
    "name": "Rate of Return",
    "termSource": "INTERNAL"
  }
}
```

## Read Terms



#### GraphQL


```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    glossaryTerms {
      terms {
        term {
          urn
          glossaryTermInfo {
            name
            description
          }
        }
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "glossaryTerms": {
        "terms": [
          {
            "term": {
              "urn": "urn:li:glossaryTerm:CustomerAccount",
              "glossaryTermInfo": {
                "name": "CustomerAccount",
                "description": "account that represents an identified, named collection of balances and cumulative totals used to summarize customer transaction-related activity over a designated period of time"
              }
            }
          }
        ]
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
--data-raw '{ "query": "{dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") {glossaryTerms {terms {term {urn glossaryTermInfo { name description } } } } } }", "variables":{}}'
```

Expected Response:

````json
{"data":{"dataset":{"glossaryTerms":{"terms":[{"term":{"urn":"urn:li:glossaryTerm:CustomerAccount","glossaryTermInfo":{"name":"CustomerAccount","description":"account that represents an identified, named collection of balances and cumulative totals used to summarize customer transaction-related activity over a designated period of time"}}}]}}},"extensions":{}}```
````



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_query_terms.py
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

print(dataset.terms)

```




## Add Terms

### Add Terms to a dataset

The following code shows you how can add terms to a dataset.
In the following code, we add a term `Rate of Return` to a dataset named `fct_users_created`.



#### GraphQL


```json
mutation addTerms {
    addTerms(
      input: {
        termUrns: ["urn:li:glossaryTerm:rateofreturn"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
      }
  )
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "addTerms": true
  },
  "extensions": {}
}
```



#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTerm { addTerms(input: { termUrns: [\"urn:li:glossaryTerm:rateofreturn\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTerms": true }, "extensions": {} }
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_term.py
from typing import List, Optional, Union

from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn


def add_terms_to_dataset(
    client: DataHubClient,
    dataset_urn: DatasetUrn,
    term_urns: List[Union[GlossaryTermUrn, str]],
) -> None:
    """
    Add glossary terms to a dataset.

    Args:
        client: DataHub client to use
        dataset_urn: URN of the dataset to update
        term_urns: List of term URNs or term names to add
    """
    dataset = client.entities.get(dataset_urn)

    for term in term_urns:
        if isinstance(term, str):
            resolved_term_urn = client.resolve.term(name=term)
            dataset.add_term(resolved_term_urn)
        else:
            dataset.add_term(term)

    client.entities.update(dataset)


def main(client: Optional[DataHubClient] = None) -> None:
    """
    Main function to add terms to dataset example.

    Args:
        client: Optional DataHub client (for testing). If not provided, creates one from env.
    """
    client = client or DataHubClient.from_env()

    dataset_urn = DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")

    # Add terms using both URN and name resolution
    add_terms_to_dataset(
        client=client,
        dataset_urn=dataset_urn,
        term_urns=[
            GlossaryTermUrn("Classification.HighlyConfidential"),
            "PII",  # Will be resolved by name
        ],
    )


if __name__ == "__main__":
    main()

```




### Add Terms to a Column of a Dataset



#### GraphQL


```json
mutation addTerms {
    addTerms(
      input: {
        termUrns: ["urn:li:glossaryTerm:rateofreturn"],
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```



#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation addTerms { addTerms(input: { termUrns: [\"urn:li:glossaryTerm:rateofreturn\"], resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\", subResourceType: DATASET_FIELD, subResource: \"user_name\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "addTerms": true }, "extensions": {} }
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_add_column_term.py
from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)

dataset["address.zipcode"].add_term(GlossaryTermUrn("Classification.Location"))

client.entities.update(dataset)

```




### Expected Outcome of Adding Terms

You can now see `Rate of Return` term has been added to `user_name` column.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/term-added.png"/>
</p>

## Remove Terms

The following code remove a term from a dataset.
After running this code, `Rate of Return` term will be removed from a `user_name` column.



#### GraphQL


```json
mutation removeTerm {
    removeTerm(
      input: {
        termUrn: "urn:li:glossaryTerm:rateofreturn",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        subResourceType:DATASET_FIELD,
        subResource:"user_name"})
}
```

Note that you can also remove a term from a dataset if you don't specify `subResourceType` and `subResource`.

```json
mutation removeTerm {
    removeTerm(
      input: {
        termUrn: "urn:li:glossaryTerm:rateofreturn",
        resourceUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
      })
}
```

Also note that you can remove terms from multiple entities or subresource using `batchRemoveTerms`.

```json
mutation batchRemoveTerms {
    batchRemoveTerms(
      input: {
        termUrns: ["urn:li:glossaryTerm:rateofreturn"],
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}
```



#### Curl


```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation removeTerm { removeTerm(input: { termUrn: \"urn:li:glossaryTerm:rateofreturn\", resourceUrn: \"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)\" }) }", "variables":{}}'
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_remove_term_execute_graphql.py
# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Query multiple aspects from entity
query = """
mutation batchRemoveTerms {
    batchRemoveTerms(
      input: {
        termUrns: ["urn:li:glossaryTerm:rateofreturn"],
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}
"""
result = graph.execute_graphql(query=query)

print(result)

```




### Expected Outcome of Removing Terms

You can now see `Rate of Return` term has been removed to `user_name` column.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/term-removed.png"/>
</p>
