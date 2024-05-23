import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data Lineage

## Why Would You Use Lineage?

Data lineage is used to capture data dependencies within an organization. It allows you to track the inputs from which a data asset is derived, along with the data assets that depend on it downstream.

For more information about data lineage, refer to [About DataHub Lineage](/docs/generated/lineage/lineage-feature-guide.md).

### Goal Of This Guide

This guide will show you how to

- Add lineage between datasets.
- Add column-level lineage between datasets.
- Read lineage.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before adding lineage, you need to ensure the targeted dataset is already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from sample ingestion.
:::

## Add Lineage

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation updateLineage {
  updateLineage(
    input: {
      edgesToAdd: [
        {
          downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
          upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
        }
      ]
      edgesToRemove: []
    }
  )
}
```

Note that you can create a list of edges. For example, if you want to assign multiple upstream entities to a downstream entity, you can do the following.

```json
mutation updateLineage {
  updateLineage(
    input: {
      edgesToAdd: [
        {
          downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
          upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
        }
        {
          downstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
          upstreamUrn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
        }
      ]
      edgesToRemove: []
    }
  )
}

```

For more information about the `updateLineage` mutation, please refer to [updateLineage](https://datahubproject.io/docs/graphql/mutations/#updatelineage).

If you see the following response, the operation was successful:

```python
{
  "data": {
    "updateLineage": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json'  --data-raw '{ "query": "mutation updateLineage { updateLineage( input:{ edgesToAdd : { downstreamUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\", upstreamUrn : \"urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)\"}, edgesToRemove :{downstreamUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\",upstreamUrn : \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\" } })}", "variables":{}}'
```

Expected Response:

```json
{ "data": { "updateLineage": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/lineage_emitter_rest.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome

You can now see the lineage between `fct_users_deleted` and `logging_events`.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/lineage-added.png"/>
</p>


## Add Column-level Lineage

<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained_sample.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcome

You can now see the column-level lineage between datasets. Note that you have to enable `Show Columns` to be able to see the column-level lineage.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/column-level-lineage-added.png"/>
</p>

## Add Lineage to Non-Dataset Entities

You can also add lineage to non-dataset entities, such as DataJobs, Charts, and Dashboards.
Please refer to the following examples.

| Connection          | Examples         | A.K.A           |
|---------------------|-------------------|-----------------|
| DataJob to DataFlow | - [lineage_job_dataflow.py](../../../metadata-ingestion/examples/library/lineage_job_dataflow.py)    | | 
| DataJob to Dataset  | - [lineage_dataset_job_dataset.py](../../../metadata-ingestion/examples/library/lineage_dataset_job_dataset.py) <br /> | Pipeline Lineage |
| Chart to Dashboard  | - [lineage_chart_dashboard.py](../../../metadata-ingestion/examples/library/lineage_chart_dashboard.py) |  |
| Chart to Dataset    | - [lineage_dataset_chart.py](../../../metadata-ingestion/examples/library/lineage_dataset_chart.py) |  |


## Read Lineage (Lineage Impact Analysis)

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
query scrollAcrossLineage {
  scrollAcrossLineage(
    input: {
      query: "*"
      urn: "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
      count: 10
      direction: DOWNSTREAM
      orFilters: [
        {
          and: [
            {
              condition: EQUAL
              negated: false
              field: "degree"
              values: ["1", "2", "3+"]
            }
          ]
        }
      ]
    }
  ) {
    searchResults {
      degree
      entity {
        urn
        type
      }
    }
  }
}
```
:::info Degree
Note that `degree` means the number of hops in the lineage. For example, `degree: 1` means the immediate downstream entities, `degree: 2` means the entities that are two hops away, and so on.
:::

The GraphQL example shows using lineage degrees as a filter, but additional search filters can be included here as well.
This will perform a multi-hop lineage search on the urn specified. For more information about the `scrollAcrossLineage` mutation, please refer to [scrollAcrossLineage](https://datahubproject.io/docs/graphql/queries/#scrollacrosslineage).


</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json'  --data-raw '{ { "query": "query scrollAcrossLineage { scrollAcrossLineage( input: { query: \"*\" urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)\" count: 10 direction: DOWNSTREAM orFilters: [ { and: [ { condition: EQUAL negated: false field: \"degree\" values: [\"1\", \"2\", \"3+\"] } ] } ] } ) { searchResults { degree entity { urn type } } }}"
}}'
```

</TabItem>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/read_lineage_execute_graphql.py show_path_as_comment }}
```
The Python SDK example shows how to read lineage of a dataset. Please note that the `aspect_type` parameter can vary depending on the entity type. 
Below is a few examples of `aspect_type` for different entities.

|Entity|Aspect_type| Reference                                                                |
|-------|------------|--------------------------------------------------------------------------|
|Dataset|`UpstreamLineageClass`| [Link](/docs/generated/metamodel/entities/dataset.md#upstreamlineage)    |
|Datajob|`DataJobInputOutputClass`| [Link](/docs/generated/metamodel/entities/dataJob.md#datajobinputoutput) |
|Dashboard|`DashboardInfoClass`| [Link](/docs/generated/metamodel/entities/dashboard.md#dashboardinfo)    |
|DataFlow|`DataFlowInfoClass`| [Link](/docs/generated/metamodel/entities/dataFlow.md#dataflowinfo)      |

Learn more about lineages of different entities in the [Add Lineage to Non-Dataset Entities](#add-lineage-to-non-dataset-entities) Section.

</TabItem>
</Tabs>


### Expected Outcome

As an outcome, you should see the downstream entities of `logging_events`.

```graphql
{
  "data": {
    "scrollAcrossLineage": {
      "searchResults": [
        {
          "degree": 1,
          "entity": {
            "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)",
            "type": "DATA_JOB"
          }
        },
        ...
        {
          "degree": 2,
          "entity": {
            "urn": "urn:li:mlPrimaryKey:(user_analytics,user_name)",
            "type": "MLPRIMARY_KEY"
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

## Read Column-level Lineage 

You can also read column-level lineage via Python SDK. 


<Tabs>
<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/read_lineage_dataset_rest.py show_path_as_comment }}

```

</TabItem>
</Tabs>

### Expected Outcome

As a response, you will get the full lineage information like this. 

```graphql
{
  "UpstreamLineageClass": {
    "upstreams": [
      {
        "UpstreamClass": {
          "auditStamp": {
            "AuditStampClass": {
              "time": 0,
              "actor": "urn:li:corpuser:unknown",
              "impersonator": null,
              "message": null
            }
          },
          "created": null,
          "dataset": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)",
          "type": "TRANSFORMED",
          "properties": null,
          "query": null
        }
      }
    ],
    "fineGrainedLineages": [
      {
        "FineGrainedLineageClass": {
          "upstreamType": "FIELD_SET",
          "upstreams": [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),browser_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"
          ],
          "downstreamType": "FIELD",
          "downstreams": [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD),browser)"
          ],
          "transformOperation": null,
          "confidenceScore": 1.0,
          "query": null
        }
      }
    ]
  }
}
```

