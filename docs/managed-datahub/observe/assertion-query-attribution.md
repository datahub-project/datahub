# Assertion Query Attribution

For certain Assertions, like Freshness and Volume, DataHub will issue queries against your Data Warehouse (ex. Snowflake) to determine if that assertion passed or failed. This can result in many additional queries against your warehouse every day, depending on how many assertions you have set up. In order to help you track and understand all of the queries coming from DataHub Cloud Observe, tagging has been added to issued queries.

## SQL Comments

For all platforms, a SQL comment is added to the top of all queries indicating the query source is DataHub and also including the URN of the assertion that issued the query. For example:

```sql
/* query_source=datahub_observe assertion_urn=urn:li:assertion:507e3dec-8fed-4809-9cdd-cf2a4a06a249 */
SELECT *
FROM users
```

## Snowflake Query Tag

For queries issued against Snowflake, a [Snowflake Query Tag](https://select.dev/posts/snowflake-query-tags) is added to the SQL statement.

```sql
ALTER SESSION SET query_tag='{"query_source": "datahub_observe", "assertion_urn": "urn:li:assertion:507e3dec-8fed-4809-9cdd-cf2a4a06a249"}'
```

## BigQuery Job Labels

BigQuery support attribution through [job labels](https://docs.cloud.google.com/bigquery/docs/adding-labels#job-label) which are automatically included in your billing data. Unfortunately, due to length and character limits on labels, the assertion URN is not included in the job label.

```python
labels = {"datahub_observe": "true"}
```
