# SQL Profiles

To install this plugin, run `pip install 'acryl-datahub[sql-profiles]'`.

The SQL-based profiler does not run alone, but rather can be enabled for other SQL-based sources.
Enabling profiling will slow down ingestion runs.

Extracts:

- row and column counts for each table
- for each column, if applicable:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

Supported SQL sources:

- AWS Athena
- BigQuery
- Druid
- Hive
- Microsoft SQL Server
- MySQL
- Oracle
- Postgres
- Redshift
- Snowflake
- Generic SQLAlchemy source

```yml
source:
  type: <sql-source> # can be bigquery, snowflake, etc - see above for the list
  config:
    # username, password, etc - varies by source type
    profiling:
      enabled: true
      limit: 1000 # optional - max rows to profile
      offset: 100 # optional - offset of first row to profile
    profile_pattern:
      deny:
        # Skip all tables ending with "_staging"
        - _staging\$
      allow:
        # Profile all tables in that start with "gold_" in "myschema"
        - myschema\.gold_.*

    # If you only want profiles (but no catalog information), set these to false
    include_tables: true
    include_views: true
```

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::