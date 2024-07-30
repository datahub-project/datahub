# DataHub Open Data Quality Assertions Specification

DataHub is developing an open-source Data Quality Assertions Specification & Compiler that will allow you to declare data quality checks / expectations / assertions using a simple, universal
YAML-based format, and then compile this into artifacts that can be registered or directly executed by 3rd party Data Quality tools like [Snowflake DMFs](https://docs.snowflake.com/en/user-guide/data-quality-intro), 
dbt tests, Great Expectations or DataHub Cloud natively. 

Ultimately, our goal is to provide an framework-agnostic, highly-portable format for defining Data Quality checks, making it seamless to swap out the underlying
assertion engine without service disruption for end consumers of the results of these data quality checks in catalogging tools like DataHub. 

## Integrations

Currently, the DataHub Open Assertions Specification supports the following integrations:

- [Snowflake DMF Assertions](snowflake/snowflake_dmfs.md)

And is looking for contributions to build out support for the following integrations:

- [Looking for Contributions] dbt tests
- [Looking for Contributions] Great Expectation checks

Below, we'll look at how to define assertions in YAML, and then provide an usage overview for each support integration.

## The Specification: Declaring Data Quality Assertions in YAML

The following assertion types are currently supported by the DataHub YAML Assertion spec:

- [Freshness](/docs/managed-datahub/observe/freshness-assertions.md) 
- [Volume](/docs/managed-datahub/observe/volume-assertions.md)
- [Column](/docs/managed-datahub/observe/column-assertions.md)
- [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md)
- [Schema](/docs/managed-datahub/observe/schema-assertions.md)

Each assertion type aims to validate a different aspect of structured table (e.g. on a data warehouse or data lake), from
structure to size to column integrity to custom metrics. 

In this section, we'll go over examples of defining each. 

### Freshness Assertions

Freshness Assertions allow you to verify that your data was updated within the expected timeframe.
Below you'll find examples of defining different types of freshness assertions via YAML. 

#### Validating that Table is Updated Every 6 Hours

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: freshness
    lookback_interval: '6 hours'
    last_modified_field: updated_at
    schedule:
      type: interval
      interval: '6 hours' # Run every 6 hours
```

This assertion checks that the `purchase_events` table in the `test_db.public` schema was updated within the last 6 hours 
by issuing a Query to the table which validates determines whether an update was made using the `updated_at` column in the past 6 hours.
To use this check, we must specify the field that contains the last modified timestamp of a given row.

The `lookback_interval` field is used to specify the "lookback window" for the assertion, whereas the `schedule` field is used to specify how often the assertion should be run.
This allows you to schedule the assertion to run at a different frequency than the lookback window, for example
to detect stale data as soon as it becomes "stale" by inspecting it more frequently.

#### Supported Source Types

Currently, the only supported `sourceType` for Freshness Assertions is `LAST_MODIFIED_FIELD`. In the future,
we may support additional source types, such as `HIGH_WATERMARK`, along with data source-specific types such as 
`AUDIT_LOG` and `INFORMATION_SCHEMA`.


### Volume Assertions

Volume Assertions allow you to verify that the number of records in your dataset meets your expectations.
Below you'll find examples of defining different types of volume assertions via YAML.

#### Validating that Tale Row Count is in Expected Range

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: volume
    metric: 'row_count'
    condition:
      type: between
      min: 1000
      max: 10000
    # filters: "event_type = 'purchase'" Optionally add filters. 
    schedule:
      type: on_table_change # Run when new data is added to the table. 
```

This assertion checks that the `purchase_events` table in the `test_db.public` schema has between 1000 and 10000 records.
Using the `condition` field, you can specify the type of comparison to be made, and the `min` and `max` fields to specify the range of values to compare against.
Using the `filters` field, you can optionally specify a SQL WHERE clause to filter the records being counted.
Using the `schedule` field you can specify when the assertion should be run, either on a fixed schedule or when new data is added to the table.
The only metric currently supported is `row_count`. 

#### Validating that Table Row Count is Less Than Value

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: volume
    metric: 'row_count'
    condition:
      type: less_than_or_equal_to
      value: 1000
    # filters: "event_type = 'purchase'" Optionally add filters. 
    schedule:
      type: on_table_change # Run when new data is added to the table. 
```

#### Validating that Table Row Count is Greater Than Value

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: volume
    metric: 'row_count'
    condition:
      type: greater_than_or_equal_to
      value: 1000
    # filters: "event_type = 'purchase'" Optionally add filters. 
    schedule:
      type: on_table_change # Run when new data is added to the table. 
```


#### Supported Conditions

The full set of supported volume assertion conditions include:

- `equal_to`
- `not_equal_to`
- `greater_than`
- `greater_than_or_equal_to`
- `less_than`
- `less_than_or_equal_to`
- `between`


### Column Assertions

Column Assertions allow you to verify that the values in a column meet your expectations.
Below you'll find examples of defining different types of column assertions via YAML.

The specification currently supports 2 types of Column Assertions:

- **Field Value**: Asserts that the values in a column meet a specific condition.
- **Field Metric**: Asserts that a specific metric aggregated across the values in a column meet a specific condition.

We'll go over examples of each below.

#### Field Values Assertion: Validating that All Column Values are In Expected Range

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: amount
    condition:
        type: between
        min: 0
        max: 10
    exclude_nulls: True
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    # failure_threshold:
    #  type: count
    #  value: 10
    schedule:
      type: on_table_change
```

This assertion checks that all values for the `amount` column in the `purchase_events` table in the `test_db.public` schema have values between 0 and 10.
Using the `field` field, you can specify the column to be asserted on, and using the `condition` field, you can specify the type of comparison to be made, 
and the `min` and `max` fields to specify the range of values to compare against.
Using the `schedule` field you can specify when the assertion should be run, either on a fixed schedule or when new data is added to the table.
Using the `filters` field, you can optionally specify a SQL WHERE clause to filter the records being counted.
Using the `exclude_nulls` field, you can specify whether to exclude NULL values from the assertion, meaning that 
NULL will simply be ignored if encountered, as opposed to failing the check. 
Using the `failure_threshold`, we can set a threshold for the number of rows that can fail the assertion before the assertion is considered failed.

#### Field Values Assertion: Validating that All Column Values are In Expected Set

The validate a VARCHAR / STRING column that should contain one of a set of values: 

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: product_id
    condition:
        type: in
        value: 
          - 'product_1'
          - 'product_2'
          - 'product_3'
    exclude_nulls: False
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    # failure_threshold:
    #  type: count
    #  value: 10
    schedule:
      type: on_table_change
```

#### Field Values Assertion: Validating that All Column Values are Email Addresses

The validate a string column contains valid email addresses:

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: email_address
    condition:
      type: matches_regex
      value: "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}"
    exclude_nulls: False
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    # failure_threshold:
    #  type: count
    #  value: 10
    schedule:
      type: on_table_change
```

#### Field Values Assertion: Supported Conditions

The full set of supported field value conditions include:

- `in`
- `not_in`
- `is_null`
- `is_not_null`
- `equal_to`
- `not_equal_to`
- `greater_than` # Numeric Only
- `greater_than_or_equal_to` # Numeric Only
- `less_than` # Numeric Only
- `less_than_or_equal_to` # Numeric Only
- `between` # Numeric Only
- `matches_regex` # String Only
- `not_empty` # String Only
- `length_greater_than` # String Only
- `length_less_than` # String Only
- `length_between` # String Only


#### Field Metric Assertion: Validating No Missing Values in Column

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: col_date
    metric: null_count
    condition:
      type: equal_to
      value: 0
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    schedule:
      type: on_table_change
```

This assertion ensures that the `col_date` column in the `purchase_events` table in the `test_db.public` schema has no NULL values.

#### Field Metric Assertion: Validating No Duplicates in Column

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: id
    metric: unique_percentage
    condition:
      type: equal_to
      value: 100
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    schedule:
      type: on_table_change
```

This assertion ensures that the `id` column in the `purchase_events` table in the `test_db.public` schema 
has no duplicates, by checking that the unique percentage is 100%.

#### Field Metric Assertion: Validating String Column is Never Empty String

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: field
    field: name
    metric: empty_percentage
    condition:
      type: equal_to
      value: 0
    # filters: "event_type = 'purchase'" Optionally add filters for Column Assertion. 
    schedule:
      type: on_table_change
```

This assertion ensures that the `name` column in the `purchase_events` table in the `test_db.public` schema is never empty, by checking that the empty percentage is 0%.

#### Field Metric Assertion: Supported Metrics

The full set of supported field metrics include:

- `null_count`
- `null_percentage`
- `unique_count`
- `unique_percentage`
- `empty_count`
- `empty_percentage`
- `min`
- `max`
- `mean`
- `median`
- `stddev`
- `negative_count`
- `negative_percentage`
- `zero_count`
- `zero_percentage`

### Field Metric Assertion: Supported Conditions

The full set of supported field metric conditions include:

- `equal_to`
- `not_equal_to`
- `greater_than`
- `greater_than_or_equal_to`
- `less_than`
- `less_than_or_equal_to`
- `between`

### Custom SQL Assertions

Custom SQL Assertions allow you to define custom SQL queries to verify your data meets your expectations.
The only condition is that the SQL query must return a single value, which will be compared against the expected value.
Below you'll find examples of defining different types of custom SQL assertions via YAML.

SQL Assertions are useful for more complex data quality checks that can't be easily expressed using the other assertion types,
and can be used to assert on custom metrics, complex aggregations, cross-table integrity checks (JOINS) or any other SQL-based data quality check.

#### Validating Foreign Key Integrity

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: sql
    statement: |
      SELECT COUNT(*)
      FROM test_db.public.purchase_events AS pe
      LEFT JOIN test_db.public.products AS p
      ON pe.product_id = p.id
      WHERE p.id IS NULL
    condition:
      type: equal_to
      value: 0
    schedule:
      type: interval
      interval: '6 hours' # Run every 6 hours
```

This assertion checks that the `purchase_events` table in the `test_db.public` schema has no rows where the `product_id` column does not have a corresponding `id` in the `products` table.

#### Comparing Row Counts Across Multiple Tables

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: sql
    statement: |
      SELECT COUNT(*) FROM test_db.public.purchase_events
      - (SELECT COUNT(*) FROM test_db.public.purchase_events_raw) AS row_count_difference
    condition:
      type: equal_to
      value: 0
    schedule:
      type: interval
      interval: '6 hours' # Run every 6 hours
```

This assertion checks that the number of rows in the `purchase_events` exactly matches the number of rows in an upstream `purchase_events_raw` table 
by subtracting the row count of the raw table from the row count of the processed table.

#### Supported Conditions

The full set of supported custom SQL assertion conditions include:

- `equal_to`
- `not_equal_to`
- `greater_than`
- `greater_than_or_equal_to`
- `less_than`
- `less_than_or_equal_to`
- `between`


### Schema Assertions (Coming Soon)

Schema Assertions allow you to define custom SQL queries to verify your data meets your expectations.
Below you'll find examples of defining different types of custom SQL assertions via YAML.

The specification currently supports 2 types of Schema Assertions:

- **Exact Match**: Asserts that the schema of a table - column names and their data types - exactly matches an expected schema
- **Contains Match** (Subset): Asserts that the schema of a table - column names and their data types - is a subset of an expected schema

#### Validating Actual Schema Exactly Equals Expected Schema

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: schema
    condition: 
      type: exact_match 
      columns:
      - name: id
        type: INTEGER
      - name: product_id
        type: STRING
      - name: amount
        type: DECIMAL
      - name: updated_at
        type: TIMESTAMP
    schedule:
      type: interval
      interval: '6 hours' # Run every 6 hours
```

This assertion checks that the `purchase_events` table in the `test_db.public` schema has the exact schema as specified, with the exact column names and data types.

#### Validating Actual Schema is Contains all of Expected Schema

```yaml
version: 1
assertions:
  - entity: urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.purchase_events,PROD)
    type: schema
    condition: 
      type: contains
      columns:
      - name: id
        type: integer
      - name: product_id
        type: string
      - name: amount
        type: number
    schedule:
      type: interval
      interval: '6 hours' # Run every 6 hours
```

This assertion checks that the `purchase_events` table in the `test_db.public` schema contains all of the columns specified in the expected schema, with the exact column names and data types.
The actual schema can also contain additional columns not specified in the expected schema.

#### Supported Data Types

The following high-level data types are currently supported by the Schema Assertion spec:

- string
- number
- boolean
- date
- timestamp
- struct
- array
- map
- union
- bytes
- enum
