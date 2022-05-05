### Prerequisites

In order to execute the `snowflake-usage` source, your Snowflake user will need to have specific privileges granted to it. Specifically, you'll need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables, using which the DataHub source extracts information. Assuming you've followed the steps outlined in `snowflake` plugin to create a DataHub-specific User & Role, you'll simply need to execute the following commands in Snowflake. This will require a user with the `ACCOUNTADMIN` role (or a role granted the IMPORT SHARES global privilege). Please see [Snowflake docs for more details](https://docs.snowflake.com/en/user-guide/data-share-consumers.html).

```sql
grant imported privileges on database snowflake to role datahub_role;
```

### Capabilities

This plugin extracts the following:

- Statistics on queries issued and tables and columns accessed (excludes views)
- Aggregation of these statistics into buckets, by day or hour granularity


:::note

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source described above.

:::

### Caveats
- Some of the features are only available in the Snowflake Enterprise Edition. This docs has notes mentioning where this applies.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc..
- If there is any [incident going on for Snowflake](https://status.snowflake.com/) we will not be able to get the metadata until that incident is resolved.
