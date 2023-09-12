### Limitations

For each table we can retrieve up to 16 MB of data, which can contain as many as 100 items. We'll further improve pagination on list tables to retrieve more items.
For the config `include_table_item`, We'll enforce the the primary keys list size not to exceed 100,
the total items we'll try to retrieve in these two scenarios:

1. If user don't specify `include_table_item`: we'll retrieve up to 100 items
2. If user specifies `include_table_item`: we'll retrieve up to 100 items plus user specified items in the table, with a total not more than 200 items
