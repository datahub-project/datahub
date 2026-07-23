# Assertion

The assertion entity represents a data quality rule that can be applied to one or more datasets. Assertions are the foundation of DataHub's data quality framework, enabling organizations to define, monitor, and enforce expectations about their data. They encompass various types of checks including field-level validation, volume monitoring, freshness tracking, schema validation, and custom SQL-based rules.

Assertions can originate from multiple sources: they can be defined natively within DataHub, ingested from external data quality tools (such as Great Expectations, dbt tests, or Snowflake Data Quality), or inferred by ML-based systems. Each assertion tracks its evaluation history over time, maintaining a complete audit trail of passes, failures, and errors.

## Identity

An **Assertion** is uniquely identified by an `assertionId`, which is a globally unique identifier that remains constant across runs of the assertion. The URN format is:

```
urn:li:assertion:<assertionId>
```

The `assertionId` is typically a generated GUID that uniquely identifies the assertion definition. For example:

```
urn:li:assertion:432475190cc846f2894b5b3aa4d55af2
```

### Generating Stable Assertion IDs

The logic for generating stable assertion IDs differs based on the source of the assertion:

- **Native Assertions**: Created in DataHub Cloud's UI or API, the platform generates a UUID
- **External Assertions**: Each integration tool generates IDs based on its own conventions:
  - **Great Expectations**: Combines expectation suite name, expectation type, and parameters
  - **dbt Tests**: Uses the test's unique_id from the manifest
  - **Snowflake Data Quality**: Uses the native DMF rule ID
- **Inferred Assertions**: ML-based systems generate IDs based on the inference model and target

The key requirement is that the same assertion definition should always produce the same `assertionId`, enabling DataHub to track the assertion's history over time even as it's re-evaluated.

## Important Capabilities

### Assertion Types

DataHub supports several types of assertions, each designed to validate different aspects of data quality:

#### 1. Field Assertions (FIELD)

Field assertions validate individual columns or fields within a dataset. They come in two sub-types:

**Field Values Assertions**: Validate that each value in a column meets certain criteria. For example:

- Values must be within a specific range
- Values must match a regex pattern
- Values must be one of a set of allowed values
- Values must not be null

**Field Metric Assertions**: Validate aggregated statistics about a column. For example:

- Null percentage must be less than 5%
- Unique count must equal row count (uniqueness check)
- Mean value must be between 0 and 100
- Standard deviation must be less than 10

<details>
<summary>Python SDK: Create a field uniqueness assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_create_field_uniqueness.py show_path_as_comment }}
```

</details>

#### 2. Volume Assertions (VOLUME)

Volume assertions monitor the amount of data in a dataset. They support several sub-types:

- **ROW_COUNT_TOTAL**: Total number of rows must meet expectations
- **ROW_COUNT_CHANGE**: Change in row count over time must meet expectations
- **INCREMENTING_SEGMENT_ROW_COUNT_TOTAL**: Latest partition/segment row count
- **INCREMENTING_SEGMENT_ROW_COUNT_CHANGE**: Change between consecutive partitions

Volume assertions are critical for detecting data pipeline failures, incomplete loads, or unexpected data growth.

<details>
<summary>Python SDK: Create a row count volume assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_create_volume_rows.py show_path_as_comment }}
```

</details>

#### 3. Freshness Assertions (FRESHNESS)

Freshness assertions ensure data is updated within expected time windows. Two types are supported:

- **DATASET_CHANGE**: Based on dataset change operations (insert, update, delete) captured from audit logs
- **DATA_JOB_RUN**: Based on successful execution of a data job

Freshness assertions define a schedule that specifies when updates should occur (e.g., daily by 9 AM, every 4 hours) and what tolerance is acceptable.

<details>
<summary>Python SDK: Create a dataset change freshness assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_create_freshness.py show_path_as_comment }}
```

</details>

#### 4. Schema Assertions (DATA_SCHEMA)

Schema assertions validate that a dataset's structure matches expectations. They verify:

- Presence or absence of specific columns
- Column data types
- Column ordering (optional)
- Schema compatibility modes:
  - **EXACT_MATCH**: Schema must match exactly
  - **SUPERSET**: Actual schema can have additional columns
  - **SUBSET**: Actual schema can have fewer columns

Schema assertions are valuable for detecting breaking changes in upstream data sources.

<details>
<summary>Python SDK: Create a schema assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_create_schema.py show_path_as_comment }}
```

</details>

#### 5. SQL Assertions (SQL)

SQL assertions allow custom validation logic using arbitrary SQL queries. Two types:

- **METRIC**: Execute SQL and assert the returned metric meets expectations
- **METRIC_CHANGE**: Assert the change in a SQL metric over time

SQL assertions provide maximum flexibility for complex validation scenarios that don't fit other assertion types, such as cross-table referential integrity checks or business rule validation.

<details>
<summary>Python SDK: Create a SQL metric assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_create_sql_metric.py show_path_as_comment }}
```

</details>

#### 6. Custom Assertions (CUSTOM)

Custom assertions provide an extension point for assertion types not directly modeled in DataHub. They're useful when:

- Integrating third-party data quality tools with unique assertion types
- Starting integration before fully mapping to DataHub's type system
- Implementing organization-specific validation logic

### Assertion Source

The `assertionInfo` aspect includes an `AssertionSource` that identifies the origin of the assertion:

- **NATIVE**: Defined directly in DataHub (DataHub Cloud feature)
- **EXTERNAL**: Ingested from external tools (Great Expectations, dbt, Snowflake, etc.)
- **INFERRED**: Generated by ML-based inference systems (DataHub Cloud feature)

External assertions should have a corresponding `dataPlatformInstance` aspect that identifies the specific platform instance they originated from.

### Assertion Run Events

Assertion evaluations are tracked using the `assertionRunEvent` timeseries aspect. Each evaluation creates a new event with:

- **timestampMillis**: When the evaluation occurred
- **runId**: Platform-specific identifier for this evaluation run
- **asserteeUrn**: The entity being asserted (typically a dataset)
- **assertionUrn**: The assertion being evaluated
- **status**: COMPLETE, RUNNING, or ERROR
- **result**: SUCCESS, FAILURE, or ERROR with details
- **batchSpec**: Optional information about the data batch evaluated
- **runtimeContext**: Optional key-value pairs with runtime parameters

Run events enable tracking assertion health over time, identifying trends, and debugging failures.

### Assertion Actions

The `assertionActions` aspect defines automated responses to assertion outcomes:

- **onSuccess**: Actions triggered when assertion passes
- **onFailure**: Actions triggered when assertion fails

Common actions include:

- Sending notifications (email, Slack, PagerDuty)
- Creating incidents
- Triggering downstream workflows
- Updating metadata

### Tags and Metadata

Like other DataHub entities, assertions support standard metadata capabilities:

- **globalTags**: Categorize and organize assertions
- **glossaryTerms**: Link assertions to business concepts
- **status**: Mark assertions as active or deprecated

<details>
<summary>Python SDK: Add tags to an assertion</summary>

```python
{{ inline /metadata-ingestion/examples/library/assertion_add_tag.py show_path_as_comment }}
```

</details>

### Standard Operators and Parameters

Assertions use a standard set of operators for comparisons:

**Numeric**: `BETWEEN`, `LESS_THAN`, `LESS_THAN_OR_EQUAL_TO`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL_TO`, `EQUAL_TO`, `NOT_EQUAL_TO`

**String**: `CONTAIN`, `START_WITH`, `END_WITH`, `REGEX_MATCH`, `IN`, `NOT_IN`

**Boolean**: `IS_TRUE`, `IS_FALSE`, `NULL`, `NOT_NULL`

**Native**: `_NATIVE_` for platform-specific operators

Parameters are provided via `AssertionStdParameters`:

- `value`: Single value for most operators
- `minValue`, `maxValue`: Range endpoints for `BETWEEN`
- Parameter types: `NUMBER`, `STRING`, `SET`

### Standard Aggregations

Field and volume assertions can apply aggregation functions before evaluation:

**Statistical**: `MEAN`, `MEDIAN`, `STDDEV`, `MIN`, `MAX`, `SUM`

**Count-based**: `ROW_COUNT`, `COLUMN_COUNT`, `UNIQUE_COUNT`, `NULL_COUNT`

**Proportional**: `UNIQUE_PROPORTION`, `NULL_PROPORTION`

**Identity**: `IDENTITY` (no aggregation), `COLUMNS` (all columns)

## Integration Points

### Relationship to Datasets

Assertions have a strong relationship with datasets through the `Asserts` relationship:

- Field assertions target specific dataset columns
- Volume assertions monitor dataset row counts
- Freshness assertions track dataset update times
- Schema assertions validate dataset structure
- SQL assertions query dataset contents

Datasets maintain a reverse relationship, showing all assertions that validate them. This enables users to understand the quality checks applied to any dataset.

### Relationship to Data Jobs

Freshness assertions can target data jobs (pipelines) to ensure they execute on schedule. When a `FreshnessAssertionInfo` has `type=DATA_JOB_RUN`, the `entity` field references a dataJob URN rather than a dataset.

### Relationship to Data Platforms

External assertions maintain a relationship to their source platform through the `dataPlatformInstance` aspect. This enables:

- Filtering assertions by source tool
- Deep-linking back to the source platform
- Understanding the assertion's external context

### GraphQL API

Assertions are fully accessible via DataHub's GraphQL API:

- Query assertions and their run history
- Create and update native assertions
- Delete assertions
- Retrieve assertions for a specific dataset

Key GraphQL types:

- `Assertion`: The main assertion entity
- `AssertionInfo`: Assertion definition and type
- `AssertionRunEvent`: Evaluation results
- `AssertionSource`: Origin metadata

### Integration with dbt

DataHub's dbt integration automatically converts dbt tests into assertions:

- **Schema Tests**: Mapped to field assertions (not_null, unique, accepted_values, relationships)
- **Data Tests**: Mapped to SQL assertions
- **Test Metadata**: Test severity, tags, and descriptions are preserved

### Integration with Great Expectations

The Great Expectations integration maps expectations to assertion types:

- Column expectations → Field assertions
- Table expectations → Volume or schema assertions
- Custom expectations → Custom assertions

Each expectation suite becomes a collection of assertions in DataHub.

### Integration with Snowflake Data Quality

Snowflake DMF (Data Metric Functions) rules are ingested as assertions:

- Row count rules → Volume assertions
- Uniqueness rules → Field metric assertions
- Freshness rules → Freshness assertions
- Custom metric rules → SQL assertions

## Notable Exceptions

### Legacy Dataset Assertion Type

The `DATASET` assertion type is a legacy format that predates the more specific field, volume, freshness, and schema assertion types. It uses `DatasetAssertionInfo` with a generic structure. New integrations should use the more specific assertion types (FIELD, VOLUME, FRESHNESS, DATA_SCHEMA, SQL) as they provide better type safety and UI rendering.

### Assertion Results vs. Assertion Metrics

While assertions track pass/fail status, DataHub also supports more detailed metrics through the `AssertionResult` object:

- `actualAggValue`: The actual value observed (for numeric assertions)
- `externalUrl`: Link to detailed results in the source system
- `nativeResults`: Platform-specific result details

This enables richer debugging and understanding of why assertions fail.

### Assertion Scheduling

DataHub tracks when assertions run through `assertionRunEvent` timeseries data, but does not directly schedule assertion evaluations. Scheduling is handled by:

- **Native Assertions**: DataHub Cloud's built-in scheduler
- **External Assertions**: The source platform's scheduler (dbt, Airflow, etc.)
- **On-Demand**: Manual or API-triggered evaluations

DataHub provides monitoring and alerting based on the assertion run events, regardless of the scheduling mechanism.

### Assertion vs. Test Results

DataHub has two related concepts:

- **Assertions**: First-class entities that define data quality rules
- **Test Results**: A simpler aspect that can be attached to datasets

Test results are lightweight pass/fail indicators without the full expressiveness of assertions. Use assertions for production data quality monitoring and test results for simple ingestion-time validation.
