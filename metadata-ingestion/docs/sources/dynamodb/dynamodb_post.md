## Advanced Configurations

### Using `include_table_item` config

If there are items that have most representative fields of the table, users could use the `include_table_item` option to provide a list of primary keys of the table in dynamodb format. We include these items in addition to the first 100 items in the table when we scan it.

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
