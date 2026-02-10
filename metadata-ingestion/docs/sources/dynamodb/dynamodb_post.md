## Advanced Configurations

### Using `schema_sampling_size` config

By default, the connector samples 100 items from each table to infer the schema. You can adjust this using the `schema_sampling_size` configuration option if you need more comprehensive schema coverage:

```yml
schema_sampling_size: 500  # Sample 500 items instead of default 100
```

### Using `include_table_item` config

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
