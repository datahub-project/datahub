### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Using `schema_sampling_size` config

By default, the connector samples 100 items from each table to infer the schema. You can adjust this using the `schema_sampling_size` configuration option if you need more comprehensive schema coverage:

```yml
# Sample 500 items instead of default 100
schema_sampling_size: 500
```

#### Using `include_table_item` config

If there are items that have most representative fields of the table, users could use the `include_table_item` option to provide a list of primary keys of the table in dynamodb format. We include these items in addition to the items sampled based on `schema_sampling_size` (default 100) when we scan the table.

Take [AWS DynamoDB Developer Guide Example tables and data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AppendixSampleTables.html) as an example, if a account has a table `Reply` in the `us-west-2` region with composite primary key `Id` and `ReplyDateTime`, users can use `include_table_item` to include 2 items as following:

Example:

```yml
# The table name should be in the format of region.table_name
# The primary keys should be in the DynamoDB format
include_table_item:
  us-west-2.Reply:
    [
      {
        "ReplyDateTime": { "S": "2015-09-22T19:58:22.947Z" },
        "Id": { "S": "Amazon DynamoDB#DynamoDB Thread 1" },
      },
      {
        "ReplyDateTime": { "S": "2015-10-05T19:58:22.947Z" },
        "Id": { "S": "Amazon DynamoDB#DynamoDB Thread 2" },
      },
    ]
```

#### Probe support

`datahub recipe probe` checks a recipe against DynamoDB without running ingestion. It offers one
command, `tables`:

```shell
datahub recipe probe methods --recipe dynamodb_recipe.yml
datahub recipe probe run tables --recipe dynamodb_recipe.yml
datahub recipe probe filter --recipe dynamodb_recipe.yml --kind Table --name us-west-2.Orders
```

`tables` lists the tables in the recipe's region with `ListTables`, the call ingestion makes, using
the recipe's own AWS credentials, role, profile and endpoint. Each name is `region.table`, the string
`table_pattern` is matched against, so `probe filter` gives ingestion's verdict. Ingestion reads one
region per run (`aws_region`), and so does the probe: a table in another region is not listed. The
probe never scans a table or reads its items, so it needs only `dynamodb:ListTables`.

Where ingestion logs a failed `ListTables` and continues as if there were no tables, the probe fails
with the AWS error code (exit 3), so a missing permission is not mistaken for an empty region.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
