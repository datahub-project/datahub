### Overview

The `dynamodb` module ingests metadata from Dynamodb into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

:::note Breaking Change
Starting v0.13.3, `aws_region` is required. The connector no longer loops through all AWS regions and only uses the region specified in your recipe.
:::

Attach the `AmazonDynamoDBReadOnlyAccess` policy to a user in your AWS account, then create an API access key and secret. The following privileges are required:

```
dynamodb:ListTables
dynamodb:DescribeTable
dynamodb:Scan
dynamodb:ListTagsOfResource
```

`dynamodb:Scan` is required because DynamoDB does not return schema information in `dynamodb:DescribeTable`. The connector samples a few values to infer the schema.

`dynamodb:ListTagsOfResource` is required only when `extract_table_tags` is enabled to extract DynamoDB table tags.

### Concept Mapping

| Source Concept | DataHub Concept                                           | Notes |
| -------------- | --------------------------------------------------------- | ----- |
| `"dynamodb"`   | [Data Platform](../../metamodel/entities/dataPlatform.md) |       |
| DynamoDB Table | [Dataset](../../metamodel/entities/dataset.md)            |       |
