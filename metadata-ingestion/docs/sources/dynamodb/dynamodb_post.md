## Limitations

For each region, the list table operation returns maximum number 100 tables, we need to further improve it by implementing pagination for listing tables

## Advanced Configurations

### Using `include_table_item` config

If there are items that have most representative fields of the table, user could use the `include_table_item` option to provide a list of primary keys of a table in dynamodb format, those items from given primary keys will be included when we scan the table.

Take [AWS DynamoDB Developer Guide Example tables and data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AppendixSampleTables.html) as an example, if user has a table `Reply` with composite primary key `Id` and `ReplyDateTime`, user can use `include_table_item` to include 2 items as following:

Example:

```yml
# put the table name and composite key in DynamoDB format
include_table_item:
  Reply:
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
