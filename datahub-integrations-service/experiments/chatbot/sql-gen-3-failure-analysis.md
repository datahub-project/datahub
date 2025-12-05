# SQL Generation Eval Failure Analysis: sql-gen-3-mau-figma

## Overview

This evaluation tested the agent's ability to generate an SQL query to calculate monthly active users (MAU) for Figma using a rolling 30-day window.

## Expected Behavior (from Response Guidelines)

The response guidelines required:

1. ✅ **Table**: `analytics_hub.public_staging.stg_datahub__due` (Snowflake DataHub Usage Events)
2. ✅ **Alternative**: Use `analytics_hub.public_core.dim_customer` to find Figma's namespace
3. ✅ **Syntax**: Use `DATEADD(day, -30, CURRENT_DATE())` for 30-day rolling window
4. ✅ **Aggregation**: Count `DISTINCT actor_urn`
5. ✅ **Filter**: `namespace = '0a290edc8e-figma'` OR indicate need to identify namespace first
6. ✅ **Output**: Working SQL query

## What the Agent Actually Did

### Table Selected

- **Used**: `usw2-saas-prod-shared-es-01.0a290edc8e-figma_datahub_usage_event` (Elasticsearch)
- **Expected**: `analytics_hub.public_staging.stg_datahub__due` (Snowflake)

### Generated SQL Query

```sql
SELECT
    DATE(t1."[version=2.0].[type=date].timestamp") AS activity_date,
    COUNT(DISTINCT t1."[version=2.0].[type=text].actorUrn") AS monthly_active_users
FROM
    "usw2-saas-prod-shared-es-01"."0a290edc8e-figma_datahub_usage_event" t1
INNER JOIN
    "usw2-saas-prod-shared-es-01"."0a290edc8e-figma_datahub_usage_event" t2
    ON t1."[version=2.0].[type=text].actorUrn" = t2."[version=2.0].[type=text].actorUrn"
    AND DATE(t2."[version=2.0].[type=date].timestamp") BETWEEN
        DATE(t1."[version=2.0].[type=date].timestamp") - INTERVAL '29 days'
        AND DATE(t1."[version=2.0].[type=date].timestamp")
WHERE
    t1."[version=2.0].[type=text].actorUrn" IS NOT NULL
GROUP BY
    DATE(t1."[version=2.0].[type=date].timestamp")
ORDER BY
    activity_date DESC
```

## Root Causes of Failure

### 1. **Wrong Data Source Platform** ❌

- **Issue**: Agent selected Elasticsearch table instead of Snowflake table
- **Why**: Smart search found Figma-specific usage event data in Elasticsearch
- **Reasoning**: Agent logically chose the table with "Figma" in the name: `0a290edc8e-figma_datahub_usage_event`
- **Impact**: Wrong platform means wrong SQL dialect and missing fields

### 2. **Wrong SQL Dialect** ❌

- **Issue**: Used `INTERVAL '29 days'` (PostgreSQL/Elasticsearch syntax)
- **Expected**: `DATEADD(day, -30, CURRENT_DATE())` (Snowflake syntax)
- **Impact**: Query won't work on Snowflake without modification

### 3. **Missing Namespace Filter** ❌

- **Issue**: No `WHERE namespace = '0a290edc8e-figma'` filter
- **Why**: Elasticsearch table is already Figma-specific (has namespace in table name)
- **Impact**: Guideline explicitly required this filter for the Snowflake table

### 4. **Overly Complex Approach** ⚠️

- **Issue**: Used self-join to calculate rolling window
- **Expected**: Simple WHERE clause with date range filter
- **Impact**: Query is functionally correct but unnecessarily complex

### 5. **Wrong Column References** ❌

- **Issue**: Used Elasticsearch nested field syntax: `"[version=2.0].[type=text].actorUrn"`
- **Expected**: Simple column: `actor_urn`
- **Impact**: Wrong schema mapping

## Why Did This Happen?

### Agent's Search Process

1. **Search Query**: "Figma user activity events usage data for calculating monthly active users"
2. **Top Result**: Elasticsearch table `0a290edc8e-figma_datahub_usage_event` with:
   - ✅ 312,806 rows of usage data
   - ✅ Tagged as "prod env"
   - ✅ Has `actorUrn`, `timestamp`, `type` fields
   - ✅ Figma-specific (namespace in name)
3. **Agent's Reasoning**: "This is the most relevant dataset because it's production, has the most data, and is Figma-specific"

### What the Agent Didn't Find

- The **Snowflake** table `stg_datahub__due` that contains **all** usage events
- This table uses a `namespace` column to filter by customer
- The agent never saw this as an option because search prioritized the Figma-specific Elasticsearch index

## Technical Assessment

### Query Correctness

- ✅ **Functionally**: The query logic is sound for calculating MAU with rolling window
- ✅ **Aggregation**: Correctly counts distinct users
- ✅ **Time Window**: 29-day window is close to 30-day requirement
- ❌ **Platform**: Wrong database platform
- ❌ **Schema**: Wrong column names
- ❌ **Syntax**: Wrong SQL dialect

### Semantic Correctness

- ✅ **Intent**: Agent understood the requirement
- ✅ **Approach**: Rolling window implementation is correct
- ❌ **Guideline Adherence**: Didn't use the expected table/syntax

## Why This Is Actually a Data Catalog Issue

The agent made a **semantically reasonable choice** but **technically incorrect choice** based on the guidelines:

1. **Figma-Specific Table Exists**: The Elasticsearch table IS the right data for Figma MAU
2. **Better Discovery**: The table is well-documented and properly tagged
3. **More Specific**: Table name explicitly contains "figma" which matched the search intent
4. **Production Data**: Has 300K+ rows of actual production usage data

**However**, the evaluation expected:

- Using the **centralized** Snowflake staging table
- Filtering by `namespace` to get Figma data
- This is a **data architecture pattern** (centralized vs. per-customer indexes)

## Recommendations

### Option 1: Update Response Guidelines to Be More Flexible

```yaml
response_guidelines: |-
  - The response should identify usage event data for Figma
  - Acceptable tables include:
    * analytics_hub.public_staging.stg_datahub__due (Snowflake) filtered by namespace
    * Figma-specific usage event tables (Elasticsearch indexes)
  - The SQL query should calculate a 30-day rolling window of active users
  - Must count DISTINCT users (actor_urn, actorUrn, or similar field)
```

### Option 2: Make the Test More Specific

```yaml
message: |-
  Generate an SQL query to calculate MAU for Figma using the Snowflake table 
  analytics_hub.public_staging.stg_datahub__due
```

### Option 3: Fix Table Discovery/Ranking

- Boost Snowflake staging tables in search results
- Add metadata to indicate "canonical" or "preferred" tables for queries
- Document that `stg_datahub__due` is the preferred source for usage analytics

## Conclusion

**The eval likely failed because**:

1. ✅ Agent found the RIGHT data (Figma usage events)
2. ❌ But used the WRONG table (Elasticsearch vs. Snowflake)
3. ❌ Generated WRONG SQL dialect (PostgreSQL vs. Snowflake)
4. ❌ Didn't match expected schema (nested fields vs. flat columns)

**The agent's behavior was**:

- **Semantically correct**: Found Figma usage data and calculated MAU correctly
- **Guideline non-compliant**: Didn't use the specific table/syntax required
- **Practically reasonable**: Chose a well-documented, production Figma-specific table

This is a **table discovery and ranking problem**, not a SQL generation problem.
