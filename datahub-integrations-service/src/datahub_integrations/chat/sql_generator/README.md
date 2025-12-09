# Text-to-SQL Generation with Semantic Model Building

This document describes the algorithm used by the `generate_sql` tool to convert natural language queries into SQL using DataHub's semantic understanding.

## Overview

The `generate_sql` tool uses a **layered semantic model building approach** to progressively enrich the context used for SQL generation. Each layer adds more semantic understanding, from basic schema information to complex table relationships inferred from query patterns.

## Algorithm: Layered Semantic Model Building

The algorithm processes tables in priority order, building a semantic model through six progressive layers:

### Layer 1: Base Schema Extraction

**What it does:**

- Fetches table metadata from DataHub (table name, database, schema)
- Retrieves column names and native data types
- Extracts column descriptions from DataHub metadata

**Data source:**

```python
dataset: Dataset = client.entities.get(urn)
qualified_name = dataset.qualified_name  # e.g., "prod.sales.orders"
columns = dataset.schema  # List of schema fields with types and descriptions
```

**Output:**

- `BaseTable`: Physical table location (database.schema.table)
- List of column names, types, and descriptions

**Limitations:**

- Quality depends on DataHub ingestion completeness
- Missing descriptions reduce LLM's ability to infer semantics
- Qualified name format assumes `database.schema.table` structure (may need handling for other platforms)

---

### Layer 2: Column Classification

**What it does:**

- Classifies each column as a **dimension**, **fact**, or **time dimension** based on data type
- Identifies enum-like columns (boolean, enum types)

**Classification rules:**

```python
if isinstance(type, (TimeTypeClass, DateTypeClass)):
    → TimeDimension  # Used for time-based analysis
elif isinstance(type, NumberTypeClass):
    → Fact  # Used for aggregations (SUM, AVG, etc.)
else:
    → Dimension  # Used for grouping and filtering
```

**Output:**

- `dimensions`: Categorical columns (strings, booleans, enums)
- `time_dimensions`: Date/timestamp columns
- `facts`: Numeric columns suitable for aggregation

**Limitations:**

- Type-based heuristic may misclassify:
  - Numeric IDs (customer_id) → classified as facts, should be dimensions
  - String amounts ("$123.45") → classified as dimensions, might be facts
- No semantic understanding yet (just type-based)
- **No column truncation for large tables:**
  - Processes ALL columns regardless of table size
  - Table with 500+ columns will cause context overflow in later steps
  - Should apply token budget like `mcp_server.py` does (see Layer 6 limitations)
- **No sample value probing (CRITICAL for key-value tables):**
  - Key-value pair tables (e.g., `app_config` with `key`, `value` columns) require knowing actual keys
  - Example problem:
    - Table: `settings` (key VARCHAR, value VARCHAR)
    - Query: "What is the timeout setting?"
    - LLM must guess: `WHERE key = 'timeout'` vs `'TIMEOUT'` vs `'app_timeout'` vs `'timeout_seconds'`
  - **Without sample values, LLM cannot know which keys exist**
  - **Solution:** Fetch sample values from DataHub `DatasetProfile` aspect:
    ```python
    # Available via graph_client.get_timeseries_values()
    field_profiles = dataset_profile.fieldProfiles
    for field in field_profiles:
        sample_values = field.sampleValues  # e.g., ["timeout", "max_retries", "api_key"]
    ```
  - Similar issues for:
    - Enum-like columns (status: 'PENDING' vs 'COMPLETED' vs 'CANCELLED'?)
    - Category codes (product_type: 'A', 'B', 'C'?)
    - Flag columns (is_active: true/false vs 1/0 vs 'Y'/'N'?)
  - See Future Enhancement #2 for implementation approach
- Could be improved with:
  - Column name pattern matching (e.g., "\_id" suffix → dimension)
  - DataHub tags/glossary terms
  - BI tool modeling (e.g., LookML dimension vs measure)
  - Priority-based truncation (PK columns > described > tagged > rest)

---

### Layer 3: Query History Fetching

**What it does:**

- Retrieves historical SQL queries that reference each table
- Prioritizes queries that involve multiple tables (for JOIN discovery)
- Sorts by query frequency/popularity

**Data source:**

```graphql
query listQueries($input: ListQueriesInput!) {
  listQueries(input: $input) {
    queries {
      urn
      properties {
        statement {
          value
        }
      }
      subjects {
        dataset {
          urn
        }
      }
    }
  }
}
```

**Query filters:**

- `source: "SYSTEM"` - Queries from BI tools/dashboards (production usage)
- `sortInput: { field: "runsPercentileLast30days", order: "DESCENDING" }` - Most frequent queries first
- `orFilters: [{ field: "entities", values: [table_urn] }]` - Queries referencing this table
- Only keeps queries with 2+ distinct tables (for JOIN inference)

**Output:**

- Up to 10 query URNs per table
- List of SQL statements for each query

**Limitations:**

- **Query history may not exist** for:
  - Newly ingested tables
  - Tables queried outside DataHub's visibility
  - Tables without SQL lineage enabled
- **SYSTEM queries only** - doesn't use MANUAL (user-written) queries
  - SYSTEM queries are more reliable (production usage)
  - But might miss ad-hoc JOIN patterns users write
- **Fixed limit** - Only fetches top 10 queries per table
  - May miss uncommon but valid JOIN patterns
  - Trade-off between context size and completeness

---

### Layer 4: JOIN Extraction from SQL Parsing

**What it does:**

- Parses each historical SQL query using `sqlglot`
- Extracts JOIN information (which tables, join columns, join conditions)
- Groups JOINs by table pairs (e.g., orders ↔ customers)

**Data source:**

```python
lineage = client._graph.parse_sql_lineage(
    sql=query_statement,
    platform=data_platform_instance.platform,
    platform_instance=data_platform_instance.instance,
    default_db=database,
    default_schema=schema,
)
joins = lineage.joins  # List of JoinInfo objects
```

**JOIN filtering:**

- Only keeps JOINs between exactly 2 tables (1 left, 1 right)
- Skips complex multi-table JOINs (3+ tables in single JOIN)
- Skips subquery JOINs and derived tables

**Output:**

- `SourcedJoinInfo` objects containing:
  - Query URN (for traceability)
  - Query text (for relationship inference)
  - `JoinInfo` (left tables, right tables, join columns)

**Limitations:**

- **SQL parsing platform-specific:**
  - `sqlglot` works best for Snowflake (well-tested)
  - BigQuery, PostgreSQL, other dialects may have edge cases
  - Complex SQL (CTEs, window functions, subqueries) may not parse correctly
- **Default database/schema assumptions:**
  - Uses `base_table.database` and `base_table.schema_name` as defaults
  - May fail for cross-database queries
  - Should ideally come from query metadata (not yet captured)
- **JOIN complexity:**
  - Only handles simple 2-table JOINs
  - Multi-way JOINs in a single statement are skipped
  - Self-joins may be mishandled
- **No JOIN condition semantics:**
  - Extracts which columns are used but not the condition type (=, <, >, etc.)
  - Assumes equality joins (most common)

---

### Layer 5: Primary Key Inference (LLM)

**What it does:**

- Uses LLM to infer primary keys for tables that don't have them in DataHub metadata
- Only runs if `logical_table.primary_key is None`

**Data source:**

- The LogicalTable built in Layers 1-2 (with classified columns)

**LLM prompt:**

```
Given the following information about a logical table, determine the primary key column(s).
There will typically be one primary key column, but there may be multiple if the table
has a composite primary key.

Columns that are foreign keys to other tables are usually not part of the primary key.
We're looking for the columns that uniquely identify each row - the grain of the table.

<logical_table>
{name, columns with types, descriptions}
</logical_table>
```

**LLM configuration:**

- Model: Uses `model_config.chat_assistant_ai.model` (typically Claude Sonnet)
- Temperature: 0.3 (low for consistent output)
- Structured output: Via `toolConfig` with `infer_primary_keys` tool spec
- Response schema: `{"columns": ["col1", "col2"]}`

**Output:**

- `PrimaryKey(columns=[...])` set on the logical table

**Limitations:**

- **LLM cost:** One API call per table without a primary key
  - Can be expensive for large schemas
  - No caching across runs (future enhancement)
- **Inference accuracy depends on:**
  - Column name clarity (e.g., "order_id" vs "id" vs "key")
  - Availability of descriptions
  - Table complexity (fact tables vs dimension tables)
- **No validation:**
  - LLM might suggest non-existent columns (should validate against schema)
  - Doesn't check if suggested columns actually have unique values
  - Could be improved with DataHub profiling statistics (uniqueness)
- **Binary decision:**
  - Either infers or uses existing
  - Doesn't handle cases where existing PK metadata is wrong

---

### Layer 6: Relationship Inference (LLM)

**What it does:**

- For each pair of tables that appear in JOINs together:
  - Uses LLM to determine relationship type (many-to-one, one-to-one)
  - Infers appropriate join type (left_outer, inner)
  - Extracts semantic description of the relationship

**Data source:**

- All `SourcedJoinInfo` objects discovered in Layer 4
- Grouped by table pairs (e.g., all JOINs between orders ↔ customers)
- Logical tables from Layers 1-2 (with column metadata)

**LLM prompt:**

```
Given the following two tables, understand the data modeling relationship between them.
You'll be given example SQL statements that join the two tables.

The extracted relationship information will serve as documentation for how other users
should join the two tables.

<logical_tables>
[{table1 with columns}, {table2 with columns}]
</logical_tables>

<example_sqls>
<example_sql>SELECT ... FROM orders LEFT JOIN customers ON orders.customer_id = customers.id</example_sql>
<example_sql>SELECT ... FROM orders JOIN customers USING (customer_id)</example_sql>
...
</example_sqls>
```

**Token management:**

- Sorts example SQLs by length (shortest first)
- Includes as many as fit within 15,000 tokens (~60KB)
- Prevents context window overflow

**LLM configuration:**

- Model: Uses `model_config.chat_assistant_ai.model`
- Temperature: 0.3
- Structured output: Via `toolConfig` with `infer_relationship` tool spec
- Response schema: `{description, relationship_type, join_type, left_table, right_table, relationship_columns}`

**Output:**

- `Relationship` objects with:
  - Semantic description (e.g., "Orders belong to customers via customer_id")
  - Cardinality (many-to-one, one-to-one)
  - Join type (left_outer, inner)
  - Column mappings (left_column ↔ right_column)

**Validation:**

- Removes relationships where:
  - Referenced table doesn't exist in the model
  - Either table lacks a primary key
  - Relationship columns aren't part of the primary key (data quality check)

**Limitations:**

- **LLM cost:** One API call per unique table pair that has JOINs
  - For 5 tables with cross-joins: potentially 10 LLM calls
  - No relationship caching (future enhancement)
- **Depends entirely on query history:**
  - If no historical JOINs exist, no relationships discovered
  - Rare JOIN patterns may be missed
- **LLM may hallucinate:**
  - Could suggest JOIN columns not found in the queries
  - May misclassify many-to-many as many-to-one
  - Should validate against actual JOIN columns from Layer 4 (TODO in code)
- **Missing relationship types:**
  - Only supports many-to-one and one-to-one
  - No many-to-many representation
  - No support for bridge tables or junction tables
- **No relationship ranking:**
  - If tables can be joined multiple ways, picks one
  - Doesn't indicate primary vs alternative join paths

---

### Priority Queue for Table Discovery

**How it works:**

- Starts with explicitly provided `table_urns`
- As JOINs are discovered in Layer 4, related tables are added to a priority queue
- Tables are processed in order of: **centrality** (how many JOINs reference them) + **popularity** (query frequency)

**Priority scoring:**

```python
centrality_score = number of JOINs involving this table
popularity_score = queryCountPercentileLast30Days (0-100)
final_score = centrality_score + popularity_score
```

**Table limits:**

- `max_tables`: Default 30, configurable
- For `generate_sql`: Set to `min(len(input_urns) * 2, 10)`
  - Allows discovering related tables
  - But prevents unbounded growth

**Why this matters:**

- A query like "top customers by revenue" might provide only `orders` table
- The algorithm discovers `customers` table via JOINs in query history
- Prioritizes highly-connected tables (e.g., dimension tables used everywhere)

**Limitations:**

- **Bounded exploration:** Stops at max_tables, may miss relevant tables
- **No query relevance:** Doesn't check if discovered tables are relevant to the user's question
  - Future: Could use semantic similarity between question and table descriptions
- **Star schema bias:** Prioritizes central fact tables, may under-represent edge dimensions

---

## SQL Generation (Final Step)

**What it does:**

- Takes the complete semantic model (tables + relationships)
- Sends to LLM with the user's natural language query
- LLM generates SQL using the semantic context

**LLM prompt structure:**

```
System: You are an expert SQL analyst. Generate SELECT queries only...

User:
<request>{natural_language_query}</request>

<semantic_model>
{
  "tables": [
    {
      "name": "orders",
      "dimensions": [...],
      "facts": [...],
      "time_dimensions": [...],
      "primary_key": {...}
    },
    ...
  ],
  "relationships": [
    {
      "left_table": "orders",
      "right_table": "customers",
      "relationship_columns": [...],
      "join_type": "left_outer",
      "relationship_type": "many_to_one"
    }
  ]
}
</semantic_model>

<platform>
{platform-specific SQL dialect hints}
</platform>

<additional_context>
{optional structured context from calling agent}
</additional_context>
```

**LLM configuration:**

- Temperature: 0.3 (low for consistent SQL)
- Structured output via `toolConfig` with schema:
  - `sql`: The generated query
  - `explanation`: What the query does
  - `confidence`: high/medium/low
  - `assumptions`: Assumptions made (e.g., "recent means last 30 days")
  - `ambiguities`: Unclear aspects (e.g., "multiple tables match 'sales'")
  - `suggested_clarifications`: Questions to ask the user

**Safety validation:**

- Regex checks for dangerous patterns: INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, GRANT, REVOKE
- If detected, confidence is lowered to "low" and warnings added to ambiguities

**Output:**

```json
{
  "sql": "SELECT ...",
  "explanation": "This query retrieves...",
  "platform": "snowflake",
  "confidence": "high",
  "tables_used": ["urn:li:dataset:..."],
  "assumptions": ["Assumed 'recent' means last 30 days"],
  "ambiguities": [],
  "suggested_clarifications": [],
  "semantic_model_summary": {
    "tables_analyzed": 3,
    "relationships_found": 2,
    "query_patterns_analyzed": 15
  }
}
```

**Limitations:**

- **LLM hallucination:**
  - May generate syntactically invalid SQL
  - Could reference non-existent columns
  - Might misunderstand complex business logic
  - No SQL execution/validation in the tool (future: syntax validation)
- **Context window limits - NO SAFEGUARDS (CRITICAL):**
  - **Tables with many columns will exceed token budget**
    - A table with 500 columns → ~50K tokens just for schema
    - 5 large tables → easily exceeds 200K context window
    - LLM call fails with context length error
  - **No column truncation:** Currently includes ALL columns from every table
    - Unlike `get_entities` which uses `ENTITY_SCHEMA_TOKEN_BUDGET` (16K per entity)
    - No priority-based column selection (PK > described > tagged)
    - No token estimation before serialization
  - **No fail-fast validation:** Tool doesn't check if semantic model fits in context
  - **Workaround:** Caller must provide fewer/smaller tables
  - **Future fix:** Apply same truncation pattern as `mcp_server.py`:
    - Sort columns by priority (PK, described, tagged, alphabetical)
    - Apply per-table token budget (~12-16K per table)
    - Add `columnsIncluded`/`columnsTruncated` metadata to response
    - Optionally: Use semantic similarity to pick query-relevant columns
- **Platform dialect variations:**
  - LLM relies on text hints for dialect-specific syntax
  - May use incorrect functions for the platform (e.g., DATE_TRUNC syntax differs)
  - Could be improved with platform-specific examples in prompt
- **Ambiguity handling:**
  - LLM makes best-guess when query is ambiguous
  - Calling agent must decide whether to ask for clarification
  - No iterative refinement within the tool itself

---

## Complete Flow Example

**Input:**

```python
generate_sql(
    natural_language_query="What were total sales by region last month?",
    table_urns=["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"],
    platform="snowflake"
)
```

**Execution:**

1. **Layer 1-2:** Fetch `orders` schema from DataHub

   - Columns: order_id (NUMBER), customer_id (NUMBER), region (VARCHAR), amount (DECIMAL), created_at (TIMESTAMP)
   - Classification: region → dimension, amount → fact, created_at → time_dimension

2. **Layer 3:** Find queries referencing `orders`

   - Query 1: `SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id`
   - Query 2: `SELECT region, SUM(amount) FROM orders WHERE created_at > '2024-01-01' GROUP BY region`
   - Query 3: `SELECT * FROM orders o LEFT JOIN products p ON o.product_id = p.id`

3. **Layer 4:** Parse JOINs from queries

   - Discovered: orders ↔ customers (via customer_id ↔ id)
   - Discovered: orders ↔ products (via product_id ↔ id)
   - Add `customers` and `products` to priority queue

4. **Layer 1-2 (again):** Fetch `customers` and `products` schemas

   - Process these tables through Layers 1-2

5. **Layer 5:** Infer primary keys for tables missing them

   - `orders`: Already has `order_id` → skip
   - `customers`: No PK in metadata → LLM infers `id`
   - `products`: No PK in metadata → LLM infers `id`

6. **Layer 6:** Infer relationships from JOINs

   - orders ↔ customers: LLM analyzes Query 1 → `many_to_one`, `left_outer`, `customer_id ↔ id`
   - orders ↔ products: LLM analyzes Query 3 → `many_to_one`, `left_outer`, `product_id ↔ id`

7. **SQL Generation:** Send semantic model + query to LLM
   - Input: "total sales by region last month" + semantic model with 3 tables, 2 relationships
   - LLM generates:
     ```sql
     SELECT
       region,
       SUM(amount) as total_sales
     FROM prod.sales.orders
     WHERE created_at >= DATEADD(month, -1, CURRENT_DATE())
       AND created_at < DATE_TRUNC('month', CURRENT_DATE())
     GROUP BY region
     ORDER BY total_sales DESC
     ```
   - Confidence: high
   - Assumptions: ["'Last month' interpreted as previous calendar month"]

---

## Integration with Snowflake Cortex Analyst

The semantic model generation can be used as a **preprocessor for Cortex Analyst**, which is Snowflake's native text-to-SQL service.

### Current State: Separate Tools

**Two distinct tools exist:**

1. **`generate_sql`** (this tool):

   - Platform-agnostic
   - Works with any DataHub-connected data warehouse
   - Uses generic LLM (Claude, GPT, etc.) for SQL generation

2. **`run_cortex_analyst`** (from `datahub-semantic-agent`):
   - Snowflake-only
   - Uses Cortex Analyst API (Snowflake's hosted LLM service)
   - Includes SQL execution capabilities

### Future Integration Strategy

**Option A: Conditional Delegation (Recommended)**

When `generate_sql` is called with `platform="snowflake"`:

```python
def generate_sql(query: str, table_urns: List[str], platform: str, ...):
    # Build semantic model (Layers 1-6)
    semantic_model = builder.build()

    # Snowflake-specific optimization
    if platform == "snowflake" and _is_cortex_analyst_available():
        # Delegate to Cortex Analyst with the semantic model
        return _generate_sql_via_cortex_analyst(
            query=query,
            semantic_model=semantic_model,  # Reuse the built model!
        )
    else:
        # Use generic LLM approach
        return _generate_sql_from_context(...)
```

**Benefits:**

- Leverages Cortex Analyst's optimizations when available
- Falls back to generic approach for other platforms
- Semantic model building is reused (no duplicate work)

**Option B: Separate Tool, Shared Builder**

Keep tools separate but extract semantic model building into a shared function:

```python
# Shared utility
def build_semantic_model_for_tables(table_urns: List[str]) -> SemanticModel:
    builder = SemanticModelBuilder(...)
    return builder.build()

# generate_sql uses it
semantic_model = build_semantic_model_for_tables(table_urns)
sql_response = _generate_sql_from_context(query, semantic_model, ...)

# run_cortex_analyst also uses it
semantic_model = build_semantic_model_for_tables(candidate_urns)
cortex_response = call_snowflake_analyst(conn, semantic_model, query)
```

**Benefits:**

- Clear separation of concerns
- Agent can choose the appropriate tool based on platform
- Easier to maintain distinct behaviors

**Option C: Execution Layer (Future)**

Add a separate `execute_sql` tool that:

- Takes SQL from `generate_sql` output
- Routes to appropriate execution engine:
  - Snowflake → Use Snowflake connector
  - BigQuery → Use BigQuery API
  - PostgreSQL → Use psycopg connection
  - Cortex Analyst → Send both query + semantic model

**Benefits:**

- Single interface for SQL execution across platforms
- Separates generation from execution
- Allows manual SQL review before execution

---

## Summary of Limitations

### Data Quality Dependencies

1. **DataHub metadata completeness:**

   - Missing descriptions reduce semantic understanding
   - Incorrect primary keys propagate through the model
   - Platform-specific type mappings may be incomplete

2. **Query history availability:**

   - New tables have no JOIN patterns
   - Ad-hoc queries may not be captured
   - Query history might be skewed toward specific use cases

3. **Schema complexity:**
   - Views and CTEs may not have proper metadata
   - Calculated columns may lack descriptions
   - Multi-database queries may fail to parse

### Algorithmic Limitations

1. **Layer 2 classification is naive:**

   - Type-based only, no semantic understanding
   - Numeric IDs misclassified as facts

2. **Layer 4 SQL parsing is brittle:**

   - Platform-specific edge cases
   - Complex queries may not parse
   - Assumes equality JOINs

3. **Layers 5-6 LLM inference:**

   - Expensive (multiple API calls)
   - May hallucinate incorrect metadata
   - No validation against actual data

4. **No iterative refinement:**
   - One-shot generation
   - Can't ask clarifying questions mid-execution
   - Returns suggestions but doesn't act on them

### Future Enhancements

1. **Add token budget management for large tables (HIGH PRIORITY):**

   - Apply same pattern as `mcp_server.py`'s `clean_get_entities_response()`
   - Use `TokenCountEstimator` to measure semantic model size before serialization
   - Per-table budget: ~12-16K tokens (similar to `ENTITY_SCHEMA_TOKEN_BUDGET`)
   - Priority-based column selection:
     ```python
     Priority:
     1. Primary key columns (essential for joins)
     2. Foreign key columns (detected from relationship_columns)
     3. Columns with descriptions (better LLM understanding)
     4. Time dimensions (crucial for temporal queries)
     5. Facts (crucial for aggregations)
     6. Dimensions (alphabetically)
     ```
   - Add truncation metadata to response:
     ```json
     "semantic_model_summary": {
       "tables_analyzed": 3,
       "columns_included": 45,
       "columns_truncated": 12,
       "truncation_applied": true
     }
     ```
   - Optional: Query-aware column selection using semantic similarity
     - Compute embedding similarity between query and column names/descriptions
     - Boost relevance scores for columns mentioned in query
     - Still respect minimum columns (PKs, FKs always included)

2. **Add DataHub profiling data (solves key-value table problem):**

   - **Sample values** (already available in `DatasetProfile` aspect):
     ```python
     # Fetch via graph_client.get_timeseries_values()
     dataset_profiles = graph_client.get_timeseries_values(
         entity_urn=urn,
         aspect_type=models.DatasetProfileClass,
         limit=1
     )
     field_profiles = dataset_profiles[0].fieldProfiles
     for field in field_profiles:
         sample_values = field.sampleValues  # List of example values
     ```
   - **Critical for key-value tables:** Knowing keys like `["timeout", "max_retries", "api_key"]` enables accurate SQL
   - **Also helps with:**
     - Enum columns (see possible status values)
     - Category codes (understand classification schemes)
     - Data format understanding (date formats, ID patterns)
   - **Existing implementation:** `gen_ai/description_context.py` has `get_sample_values()` helper
   - **Token cost:** ~5-10 tokens per sample value (should limit to 5-10 samples per column)
   - Uniqueness statistics (helps identify dimension vs fact)
   - Value distributions (for enum detection and cardinality understanding)

3. **Improve column classification:**

   - Name-based heuristics (e.g., "\*\_id" → dimension)
   - Use DataHub tags/glossary terms
   - Integrate BI tool modeling (LookML, dbt)

4. **Semantic model caching:**

   - Cache built models by table set
   - TTL-based invalidation
   - Reduces LLM costs for repeated queries

5. **SQL validation:**

   - Parse generated SQL before returning
   - Validate column references against schema
   - Estimate query cost/performance

6. **Multi-turn refinement:**

   - Allow follow-up questions to refine SQL
   - Maintain conversation context
   - Learn from user corrections

7. **Platform-specific optimizations:**
   - Template library for common patterns per platform
   - Use platform cost models to suggest optimizations
   - Leverage platform-specific features (e.g., BigQuery ML functions)
