# DataHub GraphQL CLI

The `datahub graphql` command provides a powerful interface to interact with DataHub's GraphQL API directly from the command line. This enables you to query metadata, perform mutations, and explore the GraphQL schema without writing custom applications.

## Quick Start

```shell
# Get current user info
datahub graphql --operation me

# Search for datasets
datahub graphql --operation searchAcrossEntities --variables '{"input": {"query": "users", "types": ["DATASET"]}}'

# Execute raw GraphQL
datahub graphql --query "query { me { username } }"
```

## Core Features

### 1. Schema Discovery

Discover available operations and understand their structure:

```shell
# List all available operations
datahub graphql --list-operations

# List only queries or mutations
datahub graphql --list-queries
datahub graphql --list-mutations
```

### 2. Smart Description

The `--describe` command intelligently searches for both operations and types:

```shell
# Describe an operation
datahub graphql --describe searchAcrossEntities

# Describe a GraphQL type
datahub graphql --describe SearchInput

# Describe enum types to see allowed values
datahub graphql --describe FilterOperator
```

**When both operation and type exist with same name:**

```shell
datahub graphql --describe someConflictingName
# Output:
# === OPERATION ===
# Operation: someConflictingName
# Type: Query
# ...
#
# === TYPE ===
# Type: someConflictingName
# Kind: INPUT_OBJECT
# ...
```

### 3. Recursive Type Exploration

Use `--recurse` with `--describe` to explore all nested types:

```shell
# Explore operation with all its input types
datahub graphql --describe searchAcrossEntities --recurse

# Explore type with all nested dependencies
datahub graphql --describe SearchInput --recurse
```

**Example recursive output:**

```
Operation: searchAcrossEntities
Type: Query
Description: Search across all entity types
Arguments:
  - input: SearchInput!

Input Type Details:

SearchInput:
  query: String
  types: [EntityType!]
  filters: SearchFilter

SearchFilter:
  criteria: [FacetFilterInput!]

FacetFilterInput:
  field: String! - Name of field to filter by
  values: [String!]! - Values, one of which the intended field should match
  condition: FilterOperator - Condition for the values

FilterOperator:
  EQUAL - Represents the relation: field = value
  GREATER_THAN - Represents the relation: field > value
  LESS_THAN - Represents the relation: field < value
```

### 4. Operation Execution

Execute operations by name without writing full GraphQL:

```shell
# Execute operation by name
datahub graphql --operation me

# Execute with variables
datahub graphql --operation searchAcrossEntities --variables '{"input": {"query": "datasets", "types": ["DATASET"]}}'

# Execute with variables from file
datahub graphql --operation createGroup --variables ./group-data.json
```

### 5. Raw GraphQL Execution

Execute any custom GraphQL query or mutation:

```shell
# Simple query
datahub graphql --query "query { me { username } }"

# Query with variables
datahub graphql --query "query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }" --variables '{"urn": "urn:li:corpuser:john"}'

# Query from file
datahub graphql --query ./complex-query.graphql --variables ./variables.json

# Mutation
datahub graphql --query "mutation { addTag(input: {resourceUrn: \"urn:li:dataset:...\", tagUrn: \"urn:li:tag:Important\"}) }"
```

### 6. File Support

Both queries and variables can be loaded from files:

```shell
# Load query from file
datahub graphql --query ./queries/search-datasets.graphql

# Load variables from file
datahub graphql --operation searchAcrossEntities --variables ./variables/search-params.json

# Both from files
datahub graphql --query ./query.graphql --variables ./vars.json
```

### 7. LLM-Friendly JSON Output

Use `--format json` to get structured JSON output perfect for LLM consumption:

```shell
# Get operations as JSON for LLM processing
datahub graphql --list-operations --format json

# Describe operation with complete type information
datahub graphql --describe searchAcrossEntities --recurse --format json

# Get type details in structured format
datahub graphql --describe SearchInput --format json
```

**Example JSON output for `--list-operations --format json`:**

```json
{
  "schema": {
    "queries": [
      {
        "name": "me",
        "type": "Query",
        "description": "Get current user information",
        "arguments": []
      },
      {
        "name": "searchAcrossEntities",
        "type": "Query",
        "description": "Search across all entity types",
        "arguments": [
          {
            "name": "input",
            "type": {
              "kind": "NON_NULL",
              "ofType": {
                "name": "SearchInput",
                "kind": "INPUT_OBJECT"
              }
            },
            "required": true,
            "description": "Search input parameters"
          }
        ]
      }
    ],
    "mutations": [...]
  }
}
```

**Example JSON output for `--describe searchAcrossEntities --recurse --format json`:**

```json
{
  "operation": {
    "name": "searchAcrossEntities",
    "type": "Query",
    "description": "Search across all entity types",
    "arguments": [...]
  },
  "relatedTypes": {
    "SearchInput": {
      "name": "SearchInput",
      "kind": "INPUT_OBJECT",
      "fields": [
        {
          "name": "query",
          "type": {"name": "String", "kind": "SCALAR"},
          "description": "Search query string"
        },
        {
          "name": "filters",
          "type": {"name": "SearchFilter", "kind": "INPUT_OBJECT"},
          "description": "Optional filters"
        }
      ]
    },
    "SearchFilter": {...},
    "FilterOperator": {
      "name": "FilterOperator",
      "kind": "ENUM",
      "values": [
        {
          "name": "EQUAL",
          "description": "Represents the relation: field = value",
          "deprecated": false
        }
      ]
    }
  },
  "meta": {
    "query": "searchAcrossEntities",
    "recursive": true
  }
}
```

### 8. Custom Schema Path

When introspection is disabled or for local development:

```shell
# Use local GraphQL schema files
datahub graphql --list-operations --schema-path ./local-schemas/

# Describe with custom schema
datahub graphql --describe searchAcrossEntities --schema-path ./graphql-schemas/

# Get JSON format with custom schema
datahub graphql --list-operations --schema-path ./schemas/ --format json
```

## Command Reference

### Global Options

| Option              | Type   | Description                                                    |
| ------------------- | ------ | -------------------------------------------------------------- |
| `--query`           | string | GraphQL query/mutation string or path to .graphql file         |
| `--variables`       | string | Variables as JSON string or path to .json file                 |
| `--operation`       | string | Execute named operation from DataHub's schema                  |
| `--describe`        | string | Describe operation or type (searches both)                     |
| `--recurse`         | flag   | Recursively explore nested types with --describe               |
| `--list-operations` | flag   | List all available operations                                  |
| `--list-queries`    | flag   | List available query operations                                |
| `--list-mutations`  | flag   | List available mutation operations                             |
| `--schema-path`     | string | Path to GraphQL schema files directory                         |
| `--no-pretty`       | flag   | Disable pretty-printing of JSON output (default: pretty-print) |
| `--format`          | choice | Output format: `human` (default) or `json` for LLM consumption |

### Usage Patterns

```shell
# Discovery
datahub graphql --list-operations
datahub graphql --describe <name> [--recurse]

# Execution
datahub graphql --operation <name> [--variables <json>]
datahub graphql --query <graphql> [--variables <json>]
```

## Advanced Examples

### Complex Search with Filters

```shell
datahub graphql --operation searchAcrossEntities --variables '{
  "input": {
    "query": "customer",
    "types": ["DATASET", "DASHBOARD"],
    "filters": [{
      "field": "platform",
      "values": ["mysql", "postgres"]
    }],
    "start": 0,
    "count": 20
  }
}'
```

### Adding Tags to Multiple Entities

```shell
# Add Important tag to a dataset
datahub graphql --query 'mutation AddTag($input: TagAssociationInput!) {
  addTag(input: $input)
}' --variables '{
  "input": {
    "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.users,PROD)",
    "tagUrn": "urn:li:tag:Important"
  }
}'
```

### Batch User Queries

```shell
# Get multiple users using raw GraphQL
datahub graphql --query 'query GetUsers($urns: [String!]!) {
  users: batchGet(urns: $urns) {
    ... on CorpUser {
      urn
      username
      properties {
        email
        displayName
      }
    }
  }
}' --variables '{"urns": ["urn:li:corpuser:alice", "urn:li:corpuser:bob"]}'
```

## Schema Introspection

DataHub's GraphQL CLI provides two modes for schema discovery:

### Schema Discovery Modes

1. **Live Introspection** (default): Queries the live GraphQL endpoint when no `--schema-path` is provided
2. **Local Schema Files**: Uses `.graphql` files from the specified directory when `--schema-path` is provided

**Note:** These modes are mutually exclusive with no fallback between them. If introspection fails, the command will fail with an error. If local schema files are invalid, the command will fail with an error.

### Schema File Structure

When using `--schema-path`, the directory should contain `.graphql` files with:

```graphql
# queries.graphql
extend type Query {
  me: AuthenticatedUser
  searchAcrossEntities(input: SearchInput!): SearchResults
}

# mutations.graphql
extend type Mutation {
  addTag(input: TagAssociationInput!): String
  deleteEntity(urn: String!): String
}
```

## Error Handling

The CLI provides clear error messages for common issues:

```shell
# Operation not found
datahub graphql --describe nonExistentOp
# Error: 'nonExistentOp' not found as an operation or type. Use --list-operations to see available operations or try a specific type name.

# Missing required arguments
datahub graphql --operation searchAcrossEntities
# Error: Operation 'searchAcrossEntities' requires arguments: input. Provide them using --variables '{"input": "value", ...}'

# Invalid JSON variables
datahub graphql --operation me --variables '{invalid json}'
# Error: Invalid JSON in variables: Expecting property name enclosed in double quotes
```

## Output Formats

### Pretty Printing (Default)

```json
{
  "me": {
    "corpUser": {
      "urn": "urn:li:corpuser:datahub",
      "username": "datahub"
    }
  }
}
```

### Compact Output

```shell
datahub graphql --operation me --no-pretty
{"me":{"corpUser":{"urn":"urn:li:corpuser:datahub","username":"datahub"}}}
```

## Integration Examples

### Shell Scripts

```bash
#!/bin/bash
# Get all datasets for a platform
PLATFORM="mysql"
RESULTS=$(datahub graphql --operation searchAcrossEntities --variables "{
  \"input\": {
    \"query\": \"*\",
    \"types\": [\"DATASET\"],
    \"filters\": [{\"field\": \"platform\", \"values\": [\"$PLATFORM\"]}]
  }
}" --no-pretty)

echo "Found $(echo "$RESULTS" | jq '.searchAcrossEntities.total') datasets"
```

### CI/CD Pipelines

```yaml
# GitHub Actions example
- name: Tag Important Datasets
  run: |
    datahub graphql --operation addTag --variables '{
      "input": {
        "resourceUrn": "${{ env.DATASET_URN }}",
        "tagUrn": "urn:li:tag:Production"
      }
    }'
```

## LLM Integration

The `--format json` option makes the CLI perfect for LLM integration:

### Benefits for AI Assistants

1. **Schema Understanding**: LLMs can parse the complete GraphQL schema structure
2. **Query Generation**: AI can generate accurate GraphQL queries based on available operations
3. **Type Validation**: LLMs understand required vs optional arguments and their types
4. **Documentation**: Rich descriptions and examples help AI provide better user assistance

### Use Cases

```shell
# AI assistant gets complete schema knowledge
datahub graphql --list-operations --format json | ai-assistant process-schema

# Generate queries for user requests
datahub graphql --describe searchAcrossEntities --recurse --format json | ai-helper generate-query --user-intent "find mysql tables"

# Validate user input against schema
datahub graphql --describe createGroup --format json | validate-user-input
```

### JSON Schema Benefits

- **Structured data**: No parsing of human-readable text required
- **Complete type information**: Includes GraphQL type wrappers (NON_NULL, LIST)
- **Rich metadata**: Descriptions, deprecation info, argument requirements
- **Consistent format**: Predictable structure across all operations and types
- **Recursive exploration**: Complete dependency graphs for complex types

## Tips and Best Practices

1. **Start with Discovery**: Use `--list-operations` and `--describe` to understand available operations
2. **Use --recurse**: When learning about complex operations, `--describe --recurse` shows the complete type structure
3. **LLM Integration**: Use `--format json` when building AI assistants or automation tools
4. **File-based Variables**: For complex variables, use JSON files instead of inline JSON
5. **Error Handling**: The CLI provides detailed error messages - read them carefully for debugging
6. **Schema Evolution**: Operations and types can change between DataHub versions - use discovery commands to stay current

## Troubleshooting

### Common Issues

**"Introspection not available"**: Use `--schema-path` to point to local GraphQL schema files

**"Operation not found"**: Check spelling and use `--list-operations` to see available operations

**"Type not found"**: Verify type name casing (GraphQL types are case-sensitive)

**Environment issues**: Ensure DataHub server is running and accessible at the configured endpoint
