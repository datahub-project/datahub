---
title: Operations
slug: /api/tutorials/operations
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/operations.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

## Why Would You Use Operations APIs?

The Operations APIs allow you to report operational changes that were made to a given Dataset or Table using the 'Operation' concept.
These operations may be viewed on the Dataset Profile (e.g. as last modified time), accessed via the DataHub GraphQL API, or
used as inputs to DataHub Cloud [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions.md).

### Goal Of This Guide

This guide will show you how to report and query Operations for a Dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [DataHub Quickstart Guide](/docs/quickstart.md).

:::note
Before reporting operations for a dataset, you need to ensure the targeted dataset is already present in DataHub.
:::

## Report Operations

You can use report dataset operations to DataHub using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation reportOperation {
  reportOperation(
    input: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
      operationType: INSERT
      sourceType: DATA_PROCESS
    }
  )
}
```

Where supported operation types include

- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`
- `ALTER`
- `DROP`
- `CUSTOM`

If you want to report an operation that happened at a specific time, you can also optionally provide
the `timestampMillis` field. If not provided, the current server time will be used as the operation time.

If you see the following response, the operation was successful:

```json
{
  "data": {
    "reportOperation": true
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_report_operation.py
from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

operation_type = "INSERT"
source_type = "DATA_PROCESS"  # Source of the operation (data platform or DAG task)

# Report a change operation for the Dataset.
operation_client.report_operation(
    urn=dataset_urn, operation_type=operation_type, source_type=source_type
)

```

</TabItem>
</Tabs>

## Read Operations

You can use read dataset operations to DataHub using the following APIs.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
query dataset {
    dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
        operations(
            limit: 10, filter: [], startTimeMillis: <start-timestamp-ms>, endTimeMillis: <end-timestamp-ms>
        ) {
            timestampMillis
            operationType
            sourceType
        }
    }
}
```

Where startTimeMillis and endTimeMillis are optional. By default, operations are sorted by time descending.

If you see the following response, the operation was successful:

```json
{
  "data": {
    "dataset": {
      "operations": [
        {
          "timestampMillis": 1231232332,
          "operationType": "INSERT",
          "sourceType": "DATA_PROCESS"
        }
      ]
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/dataset_read_operations.py
from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

# Query for changes to the Dataset.
operations = operation_client.query_operations(
    urn=dataset_urn,
    # limit=5,
    # start_time_millis=<timestamp>,
    # end_time_millis=<timestamo>
)

```

</TabItem>
</Tabs>

### Expected Outcomes of Reporting Operations

Reported Operations will appear when displaying the Last Updated time for a Dataset on their DataHub Profile.
They will also be used when selecting the `DataHub Operation` source type under the **Advanced** settings of a Freshness
Assertion.
