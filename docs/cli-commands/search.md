# DataHub Search Command

The `datahub search` command provides a powerful interface to search across all DataHub entities directly from the command line. It supports both keyword search (default) and semantic search with flexible filtering, multiple output formats, and comprehensive discovery features.

The search command is a **command group** with multiple subcommands:

- `query` (default) - Execute search queries across DataHub entities
- `diagnose` - Diagnose search configuration and availability

## Quick Start

```shell
# Basic keyword search (query is the default subcommand)
datahub search "users"
datahub search query "users"  # Explicit subcommand

# Search with filters
datahub search "*" --filter platform=snowflake --filter entity_type=dataset

# Semantic search
datahub search --semantic "financial reports"

# Get results in table format
datahub search "dashboard" --table

# Get just URNs for piping
datahub search "dataset" --urns-only

# Preview query without executing
datahub search "users" --filter platform=snowflake --dry-run

# Custom field projection (reduce response size)
datahub search "users" --projection "urn type platform { name }"

# Diagnose search configuration
datahub search diagnose
datahub search diagnose --format json
```

## Core Features

### 1. Dual Search Modes

**Keyword Search (Default)**: Traditional text-based search using DataHub's search index.

```shell
datahub search "users"
datahub search "customer_data"
```

**Semantic Search**: AI-powered search that understands meaning and context.

```shell
datahub search --semantic "financial reports"
datahub search --semantic "user analytics dashboards"
```

### 2. Flexible Filtering

**Simple Filters**: Easy key=value syntax with implicit AND logic.

```shell
# Single filter
datahub search "*" --filter platform=snowflake

# Multiple filters (AND)
datahub search "*" --filter platform=snowflake --filter entity_type=dataset

# OR logic with comma-separated values
datahub search "*" --filter platform=snowflake,bigquery
```

**Complex Filters**: JSON-based filters with AND/OR/NOT logic.

```shell
# AND logic
datahub search "*" --filters '{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}'

# OR logic
datahub search "*" --filters '{"or": [{"entity_type": ["chart"]}, {"entity_type": ["dashboard"]}]}'

# NOT logic
datahub search "*" --filters '{"not": {"platform": ["mysql"]}}'

# Complex nested logic
datahub search "*" --filters '{
  "and": [
    {"platform": ["snowflake", "bigquery"]},
    {"env": ["PROD"]},
    {"not": {"tag": ["urn:li:tag:Deprecated"]}}
  ]
}'
```

### 3. Multiple Output Formats

**JSON (Default)**: Structured output with full metadata.

```shell
datahub search "users" --format json
# or simply
datahub search "users"
```

**Table**: Human-readable table format.

```shell
datahub search "users" --table
# or
datahub search "users" --format table
```

**URNs**: List of URNs only, perfect for piping.

```shell
datahub search "dataset" --urns-only
# or
datahub search "dataset" --format urns
```

### 4. Filter Discovery

**List Available Filters**: See all filter types and their descriptions.

```shell
datahub search --list-filters
```

Output includes:

- entity_type, entity_subtype
- platform, domain, container
- tag, glossary_term, owner
- env, status
- custom, and, or, not

**Describe Specific Filter**: Get detailed information about a filter.

```shell
datahub search --describe-filter platform
datahub search --describe-filter entity_type
datahub search --describe-filter custom
```

### 5. Faceted Search

**Facets-Only Mode**: Get aggregation counts without search results.

```shell
datahub search "*" --facets-only

# With filters
datahub search "*" --filter entity_type=dataset --facets-only --format table
```

Useful for:

- Understanding data distribution
- Building filter UIs
- Data exploration

### 6. Pagination

```shell
# Get first 20 results
datahub search "*" --limit 20

# Get next page (results 20-40)
datahub search "*" --limit 20 --offset 20

# Maximum limit is 50
datahub search "*" --limit 50
```

### 7. Sorting

```shell
# Sort by field
datahub search "*" --sort-by lastModified

# Sort ascending
datahub search "*" --sort-by name --sort-order asc

# Sort descending (default)
datahub search "*" --sort-by name --sort-order desc
```

### 8. DataHub Views

Apply saved DataHub Views to searches:

```shell
datahub search "*" --view "urn:li:dataHubView:12345678"
```

### 9. Dry Run

Preview the compiled GraphQL query and variables without connecting to DataHub:

```shell
# See what query would be sent
datahub search "users" --dry-run

# Dry run with filters and sorting
datahub search "*" --filter platform=snowflake --sort-by lastModified --dry-run

# Dry run with semantic search
datahub search --semantic "financial data" --dry-run

# Dry run with projection
datahub search "users" --projection "urn type" --dry-run
```

Output is a JSON object containing `operation_name`, `graphql_field`, `variables`, and optionally `projection` and `query` (when using `--projection`).

### 10. Field Projection

Control which GraphQL fields are returned per entity to reduce response size:

```shell
# Return only URN and type
datahub search "users" --projection "urn type"

# Return URN with platform info
datahub search "users" --projection "urn platform { ...PlatformFields }"

# Load projection from a file
datahub search "users" --projection @my_fields.gql

# Braces are optional — these are equivalent
datahub search "users" --projection "{ urn type }"
datahub search "users" --projection "urn type"
```

When `--projection` is omitted, the full default selection set from `search_queries.gql` is used. Projections have a 5000-character limit and must not contain mutations, introspection queries, variables, or directives.

### 11. Agent Context

Print best-practice guidance for AI agents consuming the search CLI:

```shell
# Print agent context and exit
datahub search --agent-context
```

When stdout is not a TTY (e.g., piped to an AI agent), `--help` automatically appends the agent context.

## Command Reference

### Main Command

```shell
datahub search [OPTIONS] COMMAND [ARGS]
```

The `search` command is a command group. The default subcommand is `query`, so `datahub search "text"` is equivalent to `datahub search query "text"`.

### Subcommands

#### `query` (default)

Execute search queries across DataHub entities.

```shell
datahub search [QUERY] [OPTIONS]
datahub search query [QUERY] [OPTIONS]
```

**Options:**

| Option              | Type   | Description                                                 |
| ------------------- | ------ | ----------------------------------------------------------- |
| `QUERY`             | string | Search query string (default: "\*" for all entities)        |
| `--semantic`        | flag   | Use semantic search instead of keyword search               |
| `-f, --filter`      | string | Simple filter: key=value (repeatable, comma for OR)         |
| `--filters`         | string | Complex filters as JSON string (AND/OR/NOT logic)           |
| `-n, --limit`       | int    | Number of results to return (default: 10, max: 50)          |
| `--offset`          | int    | Starting position for pagination (default: 0)               |
| `--sort-by`         | string | Field name to sort by                                       |
| `--sort-order`      | choice | Sort order: `asc` or `desc` (default: desc)                 |
| `--format`          | choice | Output format: `json`, `table`, or `urns` (default: json)   |
| `--table`           | flag   | Shortcut for --format table                                 |
| `--urns-only`       | flag   | Shortcut for --format urns                                  |
| `--list-filters`    | flag   | List all available filter fields                            |
| `--describe-filter` | string | Describe a specific filter field                            |
| `--facets-only`     | flag   | Return only facets, no search results                       |
| `--view`            | string | DataHub View URN to apply                                   |
| `--dry-run`         | flag   | Show compiled GraphQL query and variables without executing |
| `--projection`      | string | Custom GraphQL selection set for entity (inline or @file)   |
| `--agent-context`   | flag   | Print agent skill context (best practices for AI agents)    |

#### `diagnose`

Diagnose search configuration and availability. Checks semantic search configuration, backend connectivity, embedding provider, model details, and enabled entity types.

```shell
datahub search diagnose [OPTIONS]
```

**Options:**

| Option     | Type   | Description                                     |
| ---------- | ------ | ----------------------------------------------- |
| `--format` | choice | Output format: `text` or `json` (default: text) |

**Examples:**

```shell
# Text output (default)
datahub search diagnose

# JSON output
datahub search diagnose --format json
```

### Available Filters

#### Entity Filters

- **entity_type**: Filter by entity type (dataset, dashboard, chart, corpuser, etc.)
- **entity_subtype**: Filter by entity subtype (Table, View, Model, etc.)

#### Platform & Location

- **platform**: Filter by data platform (snowflake, bigquery, looker, etc.)
- **container**: Filter by container URN (database, schema, etc.)
- **domain**: Filter by domain URN

#### Metadata

- **tag**: Filter by tag URN
- **glossary_term**: Filter by glossary term URN
- **owner**: Filter by owner URN (user or group)

#### Environment

- **env**: Filter by environment (PROD, DEV, STAGING, QA)
- **status**: Filter by deletion status (NOT_SOFT_DELETED, SOFT_DELETED)

#### Advanced

- **custom**: Custom field filter with flexible conditions
- **and**: Logical AND of filters
- **or**: Logical OR of filters
- **not**: Logical NOT of a filter

## Usage Examples

### Basic Searches

```shell
# Search for everything
datahub search "*"

# Search for specific term
datahub search "users"

# Search with limit
datahub search "dashboard" --limit 20
```

### Semantic Search

```shell
# Find financial datasets
datahub search --semantic "financial reports and revenue data"

# Find user analytics
datahub search --semantic "user behavior analysis dashboards"

# Find ML models
datahub search --semantic "machine learning models for prediction"
```

### Simple Filtering

```shell
# Platform filter
datahub search "*" --filter platform=snowflake

# Entity type filter
datahub search "*" --filter entity_type=dataset

# Multiple filters (AND)
datahub search "*" --filter platform=snowflake --filter env=PROD

# OR with comma separation
datahub search "*" --filter platform=snowflake,bigquery

# Combine different filters
datahub search "customer" --filter platform=mysql --filter entity_type=dataset,dashboard
```

### Complex Filtering

```shell
# Find Snowflake PROD datasets
datahub search "*" --filters '{
  "and": [
    {"platform": ["snowflake"]},
    {"entity_type": ["dataset"]},
    {"env": ["PROD"]}
  ]
}'

# Find charts OR dashboards in Looker
datahub search "*" --filters '{
  "and": [
    {"platform": ["looker"]},
    {"or": [
      {"entity_type": ["chart"]},
      {"entity_type": ["dashboard"]}
    ]}
  ]
}'

# Exclude deprecated datasets
datahub search "*" --filters '{
  "and": [
    {"entity_type": ["dataset"]},
    {"not": {"tag": ["urn:li:tag:Deprecated"]}}
  ]
}'

# Custom field filter
datahub search "*" --filters '{
  "custom": {
    "field": "name",
    "condition": "CONTAIN",
    "values": ["customer"]
  }
}'
```

### Output Formats

```shell
# JSON (default) - full metadata
datahub search "users" --format json

# Table - human readable
datahub search "users" --table

# URNs only - for piping to other commands
datahub search "*" --filter entity_type=dataset --urns-only | while read urn; do
  datahub get --urn "$urn" --aspect ownership
done
```

### Discovery & Exploration

```shell
# List all available filters
datahub search --list-filters

# Learn about a specific filter
datahub search --describe-filter platform
datahub search --describe-filter entity_type
datahub search --describe-filter custom

# Explore data distribution with facets
datahub search "*" --facets-only --format table
datahub search "*" --filter entity_type=dataset --facets-only

# Diagnose search configuration
datahub search diagnose
datahub search diagnose --format json
```

### Pagination

```shell
# First page (10 results)
datahub search "dataset"

# Second page
datahub search "dataset" --limit 10 --offset 10

# Large page
datahub search "*" --limit 50 --offset 100
```

### Sorting

```shell
# Sort by last modified (newest first)
datahub search "*" --sort-by lastModified --sort-order desc

# Sort by name (alphabetically)
datahub search "*" --sort-by name --sort-order asc
```

## Advanced Use Cases

### Finding Datasets in Multiple Platforms

```shell
# Quick version with simple filters
datahub search "*" --filter platform=snowflake,bigquery,postgres --filter entity_type=dataset --table

# Complex version with AND/OR
datahub search "*" --filters '{
  "and": [
    {"entity_type": ["dataset"]},
    {"or": [
      {"platform": ["snowflake"]},
      {"platform": ["bigquery"]},
      {"platform": ["postgres"]}
    ]}
  ]
}' --table
```

### Finding Production Data with Specific Tags

```shell
datahub search "*" --filters '{
  "and": [
    {"env": ["PROD"]},
    {"tag": ["urn:li:tag:PII", "urn:li:tag:Sensitive"]}
  ]
}' --table
```

### Finding Datasets Owned by Specific Team

```shell
datahub search "*" --filters '{
  "and": [
    {"entity_type": ["dataset"]},
    {"owner": ["urn:li:corpGroup:data-eng"]}
  ]
}' --table
```

### Pipeline: Search and Process

```shell
# Find all PROD datasets and check their ownership
datahub search "*" \
  --filter entity_type=dataset \
  --filter env=PROD \
  --urns-only | \
while read urn; do
  echo "Checking $urn"
  datahub get --urn "$urn" --aspect ownership
done

# Find datasets and export to file
datahub search "*" \
  --filter platform=snowflake \
  --format json > snowflake_datasets.json
```

### Semantic Search for Data Discovery

```shell
# Find datasets about customers
datahub search --semantic "customer information and profiles"

# Find ML models for forecasting
datahub search --semantic "predictive models and forecasting algorithms"

# Find compliance-related documentation
datahub search --semantic "GDPR compliance and privacy documentation"
```

## Output Examples

### JSON Output (Default)

```json
{
  "count": 2,
  "facets": [
    {
      "aggregations": [
        { "count": 10, "value": "DATASET" },
        { "count": 5, "value": "DASHBOARD" }
      ],
      "displayName": "Type",
      "field": "_entityType"
    }
  ],
  "searchResults": [
    {
      "entity": {
        "platform": { "name": "snowflake" },
        "properties": {
          "description": "User data table",
          "name": "users"
        },
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.users,PROD)"
      },
      "matchedFields": [{ "name": "name", "value": "users" }]
    }
  ],
  "start": 0,
  "total": 15
}
```

### Table Output

```
+--------------------------------------------------------------+------------+-----------+-------------------------+
| URN                                                          | Name       | Platform  | Description             |
+==============================================================+============+===========+=========================+
| urn:li:dataset:(urn:li:dataPlatform:snowflake,db.users,PROD)| users      | snowflake | User data table         |
+--------------------------------------------------------------+------------+-----------+-------------------------+
| urn:li:dashboard:(looker,dashboards.19)                     | User Stats | looker    | User analytics dash...  |
+--------------------------------------------------------------+------------+-----------+-------------------------+

Showing 1-2 of 15 results
```

### URNs Output

```
urn:li:dataset:(urn:li:dataPlatform:snowflake,db.users,PROD)
urn:li:dashboard:(looker,dashboards.19)
urn:li:chart:(looker,chart.123)
```

## Integration Examples

### Shell Scripts

```bash
#!/bin/bash
# Find and tag all PROD datasets in Snowflake

DATASETS=$(datahub search "*" \
  --filter platform=snowflake \
  --filter entity_type=dataset \
  --filter env=PROD \
  --urns-only)

for urn in $DATASETS; do
  echo "Tagging $urn"
  datahub put --urn "$urn" --aspect globalTags -d '{
    "tags": [{"tag": "urn:li:tag:Production"}]
  }'
done
```

### Data Quality Checks

```bash
#!/bin/bash
# Find datasets without ownership

DATASETS=$(datahub search "*" \
  --filter entity_type=dataset \
  --limit 50 \
  --format json)

echo "$DATASETS" | jq -r '.searchResults[] |
  select(.entity.ownership == null) |
  .entity.urn'
```

### Monitoring

```bash
#!/bin/bash
# Monitor new datasets added today

COUNT=$(datahub search "*" \
  --filter entity_type=dataset \
  --format json | jq '.total')

echo "Total datasets: $COUNT"
```

## Error Handling

The CLI provides clear error messages for common issues:

```shell
# Semantic search not enabled
datahub search --semantic "test"
# Error: Semantic search is not enabled on this DataHub instance.
# Use keyword search (default) or contact your DataHub administrator.

# Invalid filter format
datahub search "*" --filter invalid_format
# Error: Invalid filter: invalid_format. Expected key=value format.

# Conflicting filter options
datahub search "*" --filter platform=snowflake --filters '{"env": ["PROD"]}'
# Error: Cannot use both --filter and --filters.
# Use --filter for simple filters or --filters for complex logic.

# Invalid limit
datahub search "*" --limit 0
# Error: --limit must be at least 1

# Limit too high
datahub search "*" --limit 100
# Warning: limit capped at 50
```

## Diagnostics

The `diagnose` subcommand helps troubleshoot search configuration issues, particularly for semantic search:

```shell
# Check semantic search configuration
datahub search diagnose

# Get detailed JSON output
datahub search diagnose --format json
```

The diagnostic output includes:

- Semantic search availability and status
- Embedding provider configuration
- Model ID and embedding key
- AWS region (if applicable)
- Enabled entity types for semantic search
- Recommendations for resolving issues

## Tips and Best Practices

1. **Start Broad**: Begin with `datahub search "*"` and add filters progressively
2. **Use Discovery**: Run `--list-filters` and `--describe-filter` to learn available options
3. **Test with `--table`**: Use table format for quick exploration before JSON processing
4. **Semantic Search**: Use semantic search for natural language queries and concept-based discovery
5. **Facets First**: Use `--facets-only` to understand data distribution before filtering
6. **Pipe URNs**: Combine `--urns-only` with other `datahub` commands for bulk operations
7. **Limit Wisely**: Start with small limits during development, increase for production
8. **Save Complex Filters**: Store complex JSON filters in files for reusability
9. **Check Pagination**: Use `total` field in JSON output to determine if pagination is needed
10. **Environment Variables**: Configure `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` for easier access

## Troubleshooting

### Common Issues

**"Semantic search is not enabled"**: Your DataHub instance doesn't have semantic search configured. Use keyword search instead or contact your administrator.

**"No results found"**: Try broadening your search query or removing filters. Use `datahub search "*"` to verify connectivity.

**"Invalid filter"**: Check filter syntax. Use `--list-filters` to see available filters and `--describe-filter <name>` for details.

**Slow searches**: Use more specific filters to narrow results. Check network latency to DataHub server.

**Permission denied**: Ensure you've run `datahub init` and have valid credentials configured.

### Getting Help

```shell
# Command help
datahub search --help

# Subcommand help
datahub search query --help
datahub search diagnose --help

# List available filters
datahub search --list-filters

# Learn about specific filter
datahub search --describe-filter platform

# Diagnose search configuration
datahub search diagnose

# Check DataHub connection
datahub check plugins
```

## See Also

- [DataHub CLI](../cli.md) - Main CLI documentation
- [GraphQL Command](./graphql.md) - GraphQL query interface
- [Dataset Command](./dataset.md) - Dataset-specific operations
- [Search API Tutorial](../api/tutorials/search.md) - Programmatic search usage
