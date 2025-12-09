# Semantic Search Port from Cloud to OSS - Summary

**Date:** December 8, 2024  
**Branch:** `semantic-search-port-from-cloud`  
**Source:** `/Users/alex/work/datahub-fork` (cloud - acryl-main branch)  
**Target:** `/Users/alex/work/second/datahub-fork` (OSS - oss_master branch)

## Summary

Successfully ported semantic search functionality from the DataHub cloud version to the open-source version. This includes all Java backend code, GraphQL resolvers, configuration, tests, documentation, and POC scripts.

## Files Copied (92 total)

### Core Backend Services (7 files)
- `metadata-io/src/main/java/com/linkedin/metadata/search/SemanticSearchService.java` - Main service orchestrating semantic search
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SemanticEntitySearchService.java` - Low-level OpenSearch k-NN implementation
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SemanticEntitySearch.java` - Interface for semantic search
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/EntityTextGenerator.java` - Generates searchable text from entities
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SearchableFieldExtractor.java` - Extracts searchable fields
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SearchableTextField.java` - Model for searchable text fields
- `metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SemanticIndexConvention.java` - Index naming conventions

### Embedding Infrastructure (4 files)
- `metadata-io/src/main/java/com/linkedin/metadata/search/embedding/IntegrationsServiceEmbeddingProvider.java` - Embedding provider using IntegrationsService
- `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/index/entity/v2/EmbeddingUtils.java` - Utilities for embeddings
- `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/index/entity/v2/V2SemanticSearchMappingsBuilder.java` - OpenSearch mappings
- `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/index/entity/v2/V2SemanticSearchSettingsBuilder.java` - OpenSearch settings

### GraphQL Layer (3 files)
- `datahub-graphql-core/src/main/resources/semantic-search.acryl.graphql` - GraphQL schema
- `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchResolver.java` - Single entity resolver
- `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchAcrossEntitiesResolver.java` - Cross-entity resolver

### Spring Factories (5 files)
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/SemanticSearchServiceFactory.java`
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/SemanticEntitySearchServiceFactory.java`
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/EmbeddingProviderFactory.java`
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/EntityTextGeneratorFactory.java`
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/SearchableFieldExtractorFactory.java`

### Configuration (3 files)
- `metadata-service/configuration/src/main/java/com/linkedin/metadata/config/search/SemanticSearchConfiguration.java`
- `metadata-service/configuration/src/main/java/com/linkedin/metadata/config/search/EmbeddingsUpdateConfiguration.java`
- `metadata-service/configuration/src/main/java/com/linkedin/metadata/config/search/ModelEmbeddingConfig.java`

### Tests (14 files)
- Unit tests and integration tests for all semantic search components

### Documentation (5 files)
- `docs/tech-spec-semantic-search.md`
- `docs/pseudo-semantic-search.md`
- `docs/dev-guides/semantic-index-dual-write.md`
- `docs/dev-guides/proposal-semantic-index-race-condition.md`
- `docs/dev-guides/proposal-semantic-search-fast-copy.md`

### POC Scripts and Tools (51 files in semantic-search-poc/)
- Complete semantic search POC directory with Python scripts for testing, backfilling, and management

## Changes Made to OSS Code

### 1. GraphQLEngineFactory.java
**File:** `metadata-service/factories/src/main/java/com/linkedin/gms/factory/graphql/GraphQLEngineFactory.java`

**Changes:**
- Added import: `com.linkedin.gms.factory.search.SemanticSearchServiceFactory`
- Added import: `com.linkedin.metadata.search.SemanticSearchService`
- Added `SemanticSearchServiceFactory.class` to `@Import` annotation
- Added autowired field:
  ```java
  @Autowired
  @Qualifier("semanticSearchService")
  private SemanticSearchService semanticSearchService;
  ```
- Added to graphQLEngine() method:
  ```java
  args.setSemanticSearchService(semanticSearchService);
  ```

### 2. GmsGraphQLEngineArgs.java
**File:** `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngineArgs.java`

**Changes:**
- Added import: `com.linkedin.metadata.search.SemanticSearchService`
- Added field: `SemanticSearchService semanticSearchService;`

## Remaining Integration Steps

### CRITICAL: Add Semantic Search Resolvers to GmsGraphQLEngine

**File to modify:** `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/GmsGraphQLEngine.java`

You need to:

1. **Add the schema file** (around line 885, after other schema files):
   ```java
   .addSchema(fileBasedSchema("semantic-search.acryl.graphql"))
   ```

2. **Add semantic search resolvers** in the `configureQueryResolvers()` method (around line 996):
   ```java
   .dataFetcher("semanticSearch", new SemanticSearchResolver(semanticSearchService))
   .dataFetcher(
       "semanticSearchAcrossEntities",
       new SemanticSearchAcrossEntitiesResolver(
           semanticSearchService, null, formService, entityClient))
   ```

3. **Add imports** at the top of the file:
   ```java
   import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchResolver;
   import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchAcrossEntitiesResolver;
   import com.linkedin.metadata.search.SemanticSearchService;
   ```

4. **Add field** to the class (around line 434, with other services):
   ```java
   private final SemanticSearchService semanticSearchService;
   ```

5. **Update constructor** to accept and assign semanticSearchService (in the GmsGraphQLEngine constructor)

## System Requirements

For semantic search to work, users need:

1. **OpenSearch 2.17+** (or Elasticsearch with k-NN plugin)
   - k-NN plugin must be enabled
   - Vector dimensions: 1024 (for Cohere Embed v3)

2. **DataHub Integrations Service** configured for embeddings
   - Uses AWS Bedrock Cohere for text embedding generation
   - Or implement custom EmbeddingProvider

3. **Configuration** in application.yaml:
   ```yaml
   searchService:
     semanticSearch:
       enabled: true
       embeddingModel: cohere_embed_v3
   ```

## GraphQL Endpoints

Once integrated, these endpoints will be available:

```graphql
# Single entity type semantic search
query {
  semanticSearch(input: {
    type: DATASET
    query: "customer churn metrics"
    start: 0
    count: 10
  }) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}

# Multi-entity semantic search
query {
  semanticSearchAcrossEntities(input: {
    types: [DATASET, DASHBOARD, CHART]
    query: "revenue analysis"
    start: 0
    count: 10
  }) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}
```

## Testing

### Unit Tests
```bash
./gradlew :metadata-io:test --tests "*Semantic*"
./gradlew :datahub-graphql-core:test --tests "*Semantic*"
```

### Integration Tests (requires OpenSearch with k-NN)
```bash
./gradlew :metadata-io:integrationTest --tests "*SemanticSearchServiceIT"
./gradlew :metadata-io:integrationTest --tests "*SemanticEntitySearchIT"
```

### GraphQL Tests (from semantic-search-poc)
```bash
cd semantic-search-poc
source .venv/bin/activate  # or create with: uv sync
python test_graphql_semantic_search.py
python test_graphql_semantic_search_integration.py --query "customer data"
```

## Key Architecture Decisions

1. **Dedicated Endpoints**: Uses separate `semanticSearch` and `semanticSearchAcrossEntities` endpoints instead of adding a searchMode parameter to existing search
   
2. **Clean Separation**: Semantic and keyword search are completely isolated, enabling independent evolution

3. **Plugin-Based (Cloud) vs Direct Integration (OSS)**: 
   - Cloud version uses AcrylGraphQLPlugin to register resolvers
   - OSS version integrates directly into GmsGraphQLEngine

4. **Optional Feature**: Semantic search is optional - if not configured, the service won't start and GraphQL endpoints won't be registered

## Verification Checklist

Before committing:

- [ ] Add semantic search resolvers to GmsGraphQLEngine.java
- [ ] Build succeeds: `./gradlew build -x test`
- [ ] Unit tests pass: `./gradlew :metadata-io:test --tests "*Semantic*"`
- [ ] GraphQL schema compiles without errors
- [ ] Review all copied files for any cloud-specific hardcoded values
- [ ] Update SAAS_SPECIFIC_FILES.md to reflect that these files are now in OSS
- [ ] Test with OpenSearch k-NN configured (if available)

## Next Steps

1. **Complete the GmsGraphQLEngine integration** (see "Remaining Integration Steps" above)
2. **Build and test** the OSS version
3. **Handle any compilation errors** (check for missing imports, dependencies)
4. **Create PR** with detailed description of the changes
5. **Documentation**: Update OSS docs to mention semantic search as an optional feature
6. **Configuration guide**: Add documentation on how to enable semantic search in OSS

## Notes

- All dependencies (IntegrationsService, OpenSearch client) already exist in OSS
- The main difference is that cloud uses AWS Bedrock Cohere by default, but OSS users can implement their own EmbeddingProvider
- Consider adding configuration to disable semantic search if OpenSearch k-NN is not available

## Questions/Conflicts to Resolve

When things look conflicting, ask about:
1. Whether to make semantic search truly optional (with feature flag) or required
2. Default embedding provider for OSS (currently uses IntegrationsService)
3. Whether to port the mae-consumer semantic doc hooks (for real-time embedding updates)
4. Whether to include the dual-write mechanism for semantic indices

---

**Author:** Alex  
**Review Status:** Ready for PR review after GmsGraphQLEngine integration is complete
