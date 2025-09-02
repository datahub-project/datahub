# GraphQL Semantic Search Tests

This directory contains test files for validating the semantic search functionality through DataHub's GraphQL API.

## Test Files

### 1. `test_graphql_semantic_search.py`

A simple test that validates semantic search can be invoked through the GraphQL API and returns results.

**Features:**

- Tests both keyword and semantic search modes
- Validates GraphQL endpoint connectivity
- Displays results in formatted tables
- Checks for semantic search availability

### 2. `test_graphql_semantic_search_integration.py`

A comprehensive integration test with performance metrics and comparison analysis.

**Features:**

- Compares keyword vs semantic search results
- Measures response times
- Calculates result overlap percentages
- Provides summary statistics
- Supports single query or test suite execution

## Prerequisites

1. **DataHub Instance**: Ensure DataHub is running (default: http://localhost:8080)

2. **Semantic Search Configuration**:

   - Enable semantic search in DataHub configuration
   - Set `SEMANTIC_SEARCH_ENABLED=true`
   - Configure `SEMANTIC_SEARCH_ALLOWED_USERS` (if using authentication)

3. **OpenSearch Indices**:

   - Semantic indices must be created (`*_semantic` variants)
   - Indices should be populated with embeddings

4. **Dependencies**: Install required Python packages:
   ```bash
   cd semantic-search-poc
   pip install -r requirements.txt
   ```

## Usage

### Basic Test

```bash
# Run from the semantic-search-poc directory
cd semantic-search-poc
python test_graphql_semantic_search.py
```

### Integration Test

```bash
# Run full test suite
python test_graphql_semantic_search_integration.py

# Test single query
python test_graphql_semantic_search_integration.py --query "customer data"

# With custom URL and token
python test_graphql_semantic_search_integration.py \
  --url http://your-datahub:8080 \
  --token your-auth-token \
  --verbose
```

## Environment Variables

Configure these in a `.env` file in the semantic-search-poc directory:

```env
# DataHub connection
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_TOKEN=your-optional-token

# Semantic search configuration (server-side)
SEMANTIC_SEARCH_ENABLED=true
SEMANTIC_SEARCH_ALLOWED_USERS=urn:li:corpuser:datahub
```

## Expected Output

### Successful Semantic Search

```
✅ Semantic search is available and responding!

Query: 'customer data and user information'

Semantic Search Results
Total Results: 15
Showing: 10

┌────┬──────────┬────────────────────────┬──────────┬─────────────────────┐
│ No.│ Type     │ Name                   │ Platform │ Description         │
├────┼──────────┼────────────────────────┼──────────┼─────────────────────┤
│ 1  │ DATASET  │ customer_profiles      │ Snowflake│ Customer profile... │
│ 2  │ DATASET  │ user_activity_logs     │ Kafka    │ Real-time user...   │
└────┴──────────┴────────────────────────┴──────────┴─────────────────────┘
```

### Performance Comparison

```
Search Metrics Comparison
┌──────────────────┬─────────────────┬──────────────────┐
│ Metric           │ Keyword Search  │ Semantic Search  │
├──────────────────┼─────────────────┼──────────────────┤
│ Total Results    │ 23              │ 18               │
│ Response Time    │ 145.32 ms       │ 287.41 ms        │
│ Result Overlap   │ 5 results       │ 27.8%            │
└──────────────────┴─────────────────┴──────────────────┘
```

## Troubleshooting

### Semantic Search Not Available

If you see "Semantic search validation failed":

1. Check DataHub logs for errors
2. Verify semantic search is enabled in configuration
3. Ensure user is in the allowed users list
4. Confirm OpenSearch indices exist

### No Results Returned

1. Verify embeddings have been generated for your datasets
2. Check OpenSearch connectivity
3. Ensure semantic indices are populated

### Authentication Errors

1. Verify your token is valid
2. Check token has necessary permissions
3. Try without token for unsecured instances

## Notes

- Semantic search typically has higher latency than keyword search due to embedding generation
- Results may differ significantly between keyword and semantic search
- Semantic search is better for conceptual queries, while keyword search excels at exact matches
- The `semanticSimilarity` score (when available) indicates relevance in semantic search

## Related Files

- `test_semantic_search.py` - Tests OpenSearch directly (not GraphQL)
- `interactive_search.py` - Interactive search tool for OpenSearch
- `SEMANTIC_SEARCH_PLAN.md` - Implementation plan for semantic search
