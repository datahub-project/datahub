# Semantic Search Smoke Tests

This directory contains smoke tests for DataHub's semantic search functionality.

## Test Files

- **`test_semantic_search.py`** - End-to-end semantic search test (provider-agnostic)
- **`test_event_driven_docs.py`** - Event-driven document ingestion tests
- **`test_source_type_filtering.py`** - Source type filtering tests
- **`test_stateful_ingestion.py`** - Stateful ingestion tests
- **`test_config_fingerprint.py`** - Configuration fingerprinting tests

## Running Semantic Search Tests

### Prerequisites

DataHub must be running with semantic search enabled and an embedding provider configured.
See [Semantic Search Configuration](../../../docs/dev-guides/semantic-search/CONFIGURATION.md) for GMS setup.

### Environment Variables

The test uses [LiteLLM](https://github.com/BerriAI/litellm) which reads API keys from environment variables:

| Provider    | Environment Variable                                         | Required |
| ----------- | ------------------------------------------------------------ | -------- |
| OpenAI      | `OPENAI_API_KEY`                                             | Yes      |
| Cohere      | `COHERE_API_KEY`                                             | Yes      |
| AWS Bedrock | `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` | Yes      |

### Running the Tests

```bash
# Activate your virtual environment
source venv/bin/activate

# Run with OpenAI
ENABLE_SEMANTIC_SEARCH_TESTS=true \
OPENAI_API_KEY="sk-your-key" \
pytest tests/semantic/test_semantic_search.py -v

# Run with Cohere
ENABLE_SEMANTIC_SEARCH_TESTS=true \
COHERE_API_KEY="your-cohere-key" \
pytest tests/semantic/test_semantic_search.py -v

# Run with AWS Bedrock
ENABLE_SEMANTIC_SEARCH_TESTS=true \
AWS_PROFILE=your-profile \
pytest tests/semantic/test_semantic_search.py -v
```

The test is **provider-agnostic** - it fetches embedding configuration from the GMS server via the AppConfig API and works with any configured provider.

## Troubleshooting

### Test is skipped

```
SKIPPED [1] semantic search tests disabled
```

**Solution:** Set `ENABLE_SEMANTIC_SEARCH_TESTS=true`

### No semanticContent found

**Possible causes:**

1. Embedding provider credentials not set (check env vars above)
2. Semantic search not enabled on the server
3. Provider mismatch between GMS config and test environment

### Semantic search returns no results

**Possible causes:**

1. Semantic indices not created/populated
2. Indexing hasn't completed - increase `INDEXING_WAIT_SECONDS` in the test

## References

- [Semantic Search Configuration](../../../docs/dev-guides/semantic-search/CONFIGURATION.md)
- [Switching Embedding Providers](../../../docs/dev-guides/semantic-search/SWITCHING_PROVIDERS.md)
