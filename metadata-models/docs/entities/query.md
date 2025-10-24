# Query

The query entity represents SQL queries (or queries in other languages) that have been executed against one or more data assets such as datasets, tables, or views. Query entities capture both manually created queries and queries discovered through automated crawling of query logs from data platforms like BigQuery, Snowflake, Redshift, and others.

Queries are powerful building blocks for understanding data lineage, usage patterns, and relationships between datasets. When DataHub ingests query logs from data warehouses, it automatically creates query entities that capture the SQL statements, the datasets they reference, execution statistics, and usage patterns over time.

## Identity

Query entities are identified by a single piece of information:

- A unique identifier (`id`) that serves as the key for the query entity. This identifier is typically generated as a hash of the normalized query text, ensuring that identical queries are deduplicated and treated as the same entity.

An example of a query identifier is `urn:li:query:3b8d7b8c7e4e8b4e3c2e1a5c6d7e8f9a`. The identifier is a unique string that can be generated through various means:

- A hash of the normalized SQL query text (common for system-discovered queries)
- A user-provided identifier (for manually created queries)
- A platform-specific query identifier

## Important Capabilities

### Query Properties

The `queryProperties` aspect contains the core information about a query:

- **Statement**: The actual query text and its language (SQL, or UNKNOWN)
- **Source**: How the query was discovered (`MANUAL` for user-entered queries via UI, or `SYSTEM` for queries discovered by crawlers)
- **Name**: Optional display name to identify the query in a human-readable way
- **Description**: Optional description providing context about what the query does
- **Created/Modified**: Audit stamps tracking who created and last modified the query, along with timestamps
- **Origin**: The source entity that this query came from (e.g., a View, Stored Procedure, dbt Model, etc.)
- **Custom Properties**: Additional key-value pairs for platform-specific metadata

The following code snippet shows you how to create a query entity with properties.

<details>
<summary>Python SDK: Create a query with properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_create.py show_path_as_comment }}
```

</details>

You can also update specific properties of an existing query:

<details>
<summary>Python SDK: Update query properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_update_properties.py show_path_as_comment }}
```

</details>

### Query Subjects

The `querySubjects` aspect captures the data assets that are referenced by a query. These are the datasets, tables, views, or other entities that the query reads from or writes to.

In single-asset queries (e.g., `SELECT * FROM table`), the subjects will contain a single table reference. In multi-asset queries (e.g., joins across multiple tables), the subjects may contain multiple table references.

<details>
<summary>Python SDK: Add subjects to a query</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_add_subjects.py show_path_as_comment }}
```

</details>

### Query Usage Statistics

The `queryUsageStatistics` aspect is a timeseries aspect that tracks execution statistics and usage patterns for queries over time. This includes:

- **Query Count**: Total number of times the query was executed in a time bucket
- **Query Cost**: The compute cost associated with executing the query (platform-specific)
- **Last Executed At**: Timestamp of the most recent execution
- **Unique User Count**: Number of distinct users who executed the query
- **User Counts**: Breakdown of execution counts by individual users

This aspect is typically populated automatically by ingestion connectors that process query logs from data platforms. The timeseries nature allows for tracking trends and patterns in query usage over time.

### Tags and Glossary Terms

Like other DataHub entities, queries can have Tags or Terms attached to them. Tags are informal labels for categorizing queries (e.g., "production", "experimental", "deprecated"), while Terms are formal vocabulary from a business glossary (e.g., "Customer Data", "Financial Reporting").

#### Adding Tags to a Query

Tags are added to queries using the `globalTags` aspect.

<details>
<summary>Python SDK: Add a tag to a query</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_add_tag.py show_path_as_comment }}
```

</details>

#### Adding Glossary Terms to a Query

Terms are added using the `glossaryTerms` aspect.

<details>
<summary>Python SDK: Add a term to a query</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_add_term.py show_path_as_comment }}
```

</details>

### Ownership

Ownership is associated to a query using the `ownership` aspect. Owners can be of different types such as `TECHNICAL_OWNER`, `BUSINESS_OWNER`, `DATA_STEWARD`, etc. Ownership helps identify who is responsible for maintaining and understanding specific queries.

<details>
<summary>Python SDK: Add an owner to a query</summary>

```python
{{ inline /metadata-ingestion/examples/library/query_add_owner.py show_path_as_comment }}
```

</details>

### Platform Instance

The `dataPlatformInstance` aspect allows you to specify which specific instance of a data platform the query is associated with. This is useful when you have multiple instances of the same platform (e.g., multiple Snowflake accounts or BigQuery projects).

## Integration Points

### Relationship with Datasets

Queries have a fundamental relationship with dataset entities through the `querySubjects` aspect. Each subject in a query references a dataset URN, creating a bidirectional relationship that allows you to:

- Navigate from a query to the datasets it references
- Navigate from a dataset to all queries that reference it

This relationship is crucial for understanding dataset usage and query-based lineage.

### Lineage Integration

Queries play a central role in DataHub's lineage capabilities:

- **Query-based Lineage**: When DataHub processes SQL queries (either from query logs or manually provided), it performs SQL parsing to extract column-level lineage information. This lineage is then attached to datasets, showing how data flows from source columns to destination columns through SQL transformations.

- **Fine-grained Lineage**: Queries can be referenced in fine-grained lineage edges on datasets, providing the SQL context for how specific columns are derived. The query URN is stored in the `query` field of fine-grained lineage information.

- **Origin Tracking**: Queries can have an `origin` field pointing to the entity they came from (e.g., a View or Stored Procedure), creating a traceable chain from the query execution back to its source definition.

### Ingestion Sources

Several DataHub ingestion connectors automatically discover and create query entities:

- **BigQuery**: Extracts queries from audit logs and information schema
- **Snowflake**: Processes query history from account usage views
- **Redshift**: Reads from system tables like `STL_QUERY` and `SVL_QUERY`
- **SQL Queries Source**: A generic connector that can process query logs from any SQL database
- **Mode**: Extracts queries from Mode reports and analyses
- **Hex**: Discovers queries from Hex notebook cells

These connectors typically:

1. Fetch query logs with SQL statements and metadata
2. Parse the SQL to identify referenced tables
3. Create query entities with appropriate properties and subjects
4. Generate usage statistics as timeseries data
5. Emit column-level lineage derived from SQL parsing

### GraphQL API

Queries can be created, updated, and deleted through the DataHub GraphQL API:

- **createQuery**: Creates a new query with specified properties and subjects
- **updateQuery**: Updates an existing query's name, description, or statement
- **deleteQuery**: Soft-deletes a query entity

These mutations are available through the GraphQL endpoint and are used by the DataHub UI for manual query management.

### Usage Analytics

Query entities contribute to dataset usage analytics. When query usage statistics are ingested, they:

- Increment the dataset's usage counts
- Track which users are querying which datasets
- Provide insights into query patterns and frequency
- Help identify high-value datasets based on query activity

## Notable Exceptions

### Query Deduplication

Queries are automatically deduplicated based on their normalized query text. This means that:

- Whitespace differences are ignored
- Comments are typically removed during normalization
- Identical queries from different users or time periods are merged into a single query entity
- Usage statistics are aggregated across all executions of the same normalized query

This deduplication is essential for managing the volume of queries in large-scale deployments.

### Temporary Tables

When processing queries that involve temporary tables, the SQL parsing aggregator maintains session context to:

- Track temporary table creation and usage within a session
- Resolve lineage through temporary tables to underlying permanent tables
- Avoid creating query subjects that reference ephemeral temporary tables

This ensures that query lineage reflects the actual data dependencies rather than intermediate temporary structures.

### Query Size Limits

Very large query statements (e.g., generated queries with thousands of lines) may be truncated or rejected to maintain system performance. The exact limits depend on the backend configuration and the storage layer.

### Language Support

Currently, query entities primarily support SQL as the query language. While there is an `UNKNOWN` language option, DataHub's SQL parsing and lineage extraction capabilities are specifically designed for SQL dialects. Other query languages (e.g., Cypher, SPARQL, or proprietary query languages) can be stored but will not benefit from automatic lineage extraction.

### Manual vs System Queries

Queries can have two sources:

- `MANUAL`: Queries created by users through the DataHub UI or API
- `SYSTEM`: Queries discovered automatically by ingestion connectors

This distinction helps differentiate between user-curated queries (which might be documented and named) and the potentially large volume of queries automatically discovered from query logs.
