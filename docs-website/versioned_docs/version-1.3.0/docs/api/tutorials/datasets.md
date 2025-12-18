---
title: Dataset
slug: /api/tutorials/datasets
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/datasets.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dataset

## Why Would You Use Datasets?

The dataset entity is one the most important entities in the metadata model. They represent collections of data that are typically represented as Tables or Views in a database (e.g. BigQuery, Snowflake, Redshift etc.), Streams in a stream-processing environment (Kafka, Pulsar etc.), bundles of data found as Files or Folders in data lake systems (S3, ADLS, etc.).
For more information about datasets, refer to our [dataset reference](/docs/generated/metamodel/entities/dataset.md).

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
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/DatasetAdd.java
package io.datahubproject.examples;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.schema.DateType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DatasetAdd {

  private DatasetAdd() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    DatasetUrn datasetUrn = UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD");
    CorpuserUrn userUrn = new CorpuserUrn("ingestion");
    AuditStamp lastModified = new AuditStamp().setTime(1640692800000L).setActor(userUrn);

    SchemaMetadata schemaMetadata =
        new SchemaMetadata()
            .setSchemaName("customer")
            .setPlatform(new DataPlatformUrn("hive"))
            .setVersion(0L)
            .setHash("")
            .setPlatformSchema(
                SchemaMetadata.PlatformSchema.create(
                    new OtherSchema().setRawSchema("__insert raw schema here__")))
            .setLastModified(lastModified);

    SchemaFieldArray fields = new SchemaFieldArray();

    SchemaField field1 =
        new SchemaField()
            .setFieldPath("address.zipcode")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("VARCHAR(50)")
            .setDescription(
                "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States")
            .setLastModified(lastModified);
    fields.add(field1);

    SchemaField field2 =
        new SchemaField()
            .setFieldPath("address.street")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("VARCHAR(100)")
            .setDescription("Street corresponding to the address")
            .setLastModified(lastModified);
    fields.add(field2);

    SchemaField field3 =
        new SchemaField()
            .setFieldPath("last_sold_date")
            .setType(
                new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType())))
            .setNativeDataType("Date")
            .setDescription("Date of the last sale date for this property")
            .setLastModified(lastModified);
    fields.add(field3);

    schemaMetadata.setFields(fields);

    MetadataChangeProposalWrapper mcpw =
        MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(datasetUrn)
            .upsert()
            .aspect(schemaMetadata)
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response = emitter.emit(mcpw, null);
    System.out.println(response.get().getResponseContent());
  }
}

```

</TabItem>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/dataset_schema.py
from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    platform="hive",
    name="realestate_db.sales",
    schema=[
        # tuples of (field name / field path, data type, description)
        (
            "address.zipcode",
            "varchar(50)",
            "This is the zipcode of the address. Specified using extended form and limited to addresses in the United States",
        ),
        ("address.street", "varchar(100)", "Street corresponding to the address"),
        ("last_sold_date", "date", "Date of the last sale date for this property"),
    ],
)

client.entities.upsert(dataset)

```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Dataset

You can now see `realestate_db.sales` dataset has been created.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-created.png"/>
</p>

## Delete Dataset

You may want to delete a dataset if it is no longer needed, contains incorrect or sensitive information, or if it was created for testing purposes and is no longer necessary in production.
It is possible to [delete entities via CLI](/docs/how/delete-metadata.md), but a programmatic approach is necessary for scalability.

There are two methods of deletion: soft delete and hard delete.
**Soft delete** sets the Status aspect of the entity to Removed, which hides the entity and all its aspects from being returned by the UI.
**Hard delete** physically deletes all rows for all aspects of the entity.

For more information about soft delete and hard delete, please refer to [Removing Metadata from DataHub](/docs/how/delete-metadata.md#delete-by-urn).

<Tabs>
<TabItem value="graphql" label="GraphQL">

> 🚫 Hard delete with `graphql` is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information.

```json
mutation batchUpdateSoftDeleted {
    batchUpdateSoftDeleted(input:
      { urns: ["urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"],
        deleted: true })
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "batchUpdateSoftDeleted": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation batchUpdateSoftDeleted { batchUpdateSoftDeleted(input: { deleted: true, urns: [\"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\"] }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "batchUpdateSoftDeleted": true }, "extensions": {} }
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
# Inlined from /metadata-ingestion/examples/library/delete_dataset.py
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

dataset_urn = make_dataset_urn(name="fct_users_created", platform="hive")

# Soft-delete the dataset.
graph.delete_entity(urn=dataset_urn, hard=False)

print(f"Deleted dataset {dataset_urn}")

```

</TabItem>
</Tabs>

### Expected Outcomes of Deleting Dataset

The dataset `fct_users_deleted` has now been deleted, so if you search for a hive dataset named `fct_users_delete`, you will no longer be able to see it.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/dataset-deleted.png"/>
</p>
