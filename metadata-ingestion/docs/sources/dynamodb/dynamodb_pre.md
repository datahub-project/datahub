### Prerequisities

Notice of breaking change: Starting v0.13.3, `aws_region` is now a required configuration for DynamoDB Connector. The connector will no longer loop through all AWS regions; instead, it will only use the region passed into the recipe configuration.

In order to execute this source, you need to attach the `AmazonDynamoDBReadOnlyAccess` policy to a user in your AWS account. Then create an API access key and secret for the user. This future proofs it in case we need to make further changes. But you can use these privileges to run this source for now

```
dynamodb:ListTables
dynamodb:DescribeTable
dynamodb:Scan
```

We need `dynamodb:Scan` because Dynamodb does not return the schema in `dynamodb:DescribeTable` and thus we sample few values to understand the schema.

### Concept Mapping

| Source Concept | DataHub Concept                                           | Notes |
| -------------- | --------------------------------------------------------- | ----- |
| `"dynamodb"`   | [Data Platform](../../metamodel/entities/dataPlatform.md) |       |
| DynamoDB Table | [Dataset](../../metamodel/entities/dataset.md)            |       |
