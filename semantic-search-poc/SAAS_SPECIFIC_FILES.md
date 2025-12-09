# SaaS-Specific Files for Semantic Search

This document lists all files that are specific to DataHub SaaS and should NOT be merged back to the open-source DataHub repository.

## Core Semantic Search Implementation

### Service Layer

- `/metadata-io/src/main/java/com/linkedin/metadata/search/SemanticSearchService.java`
  - Main service orchestrating semantic search with facet attachment from keyword search
- `/metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SemanticEntitySearchService.java`
  - Low-level implementation using OpenSearch k-NN for vector similarity search
- `/metadata-io/src/main/java/com/linkedin/metadata/search/semantic/SemanticEntitySearch.java`
  - Interface defining semantic search contract

### Embedding Infrastructure

- `/metadata-io/src/main/java/com/linkedin/metadata/search/embedding/EmbeddingProvider.java`
  - Interface for embedding generation
- `/metadata-io/src/main/java/com/linkedin/metadata/search/embedding/IntegrationsServiceEmbeddingProvider.java`
  - Implementation using DataHub Integrations Service with AWS Bedrock Cohere

### GraphQL Layer

- `/datahub-graphql-core/src/main/resources/semantic-search.acryl.graphql`
  - GraphQL schema defining semantic search endpoints
- `/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchResolver.java`
  - Resolver for single-entity semantic search
- `/datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchAcrossEntitiesResolver.java`
  - Resolver for cross-entity semantic search

### Spring Factories

- `/metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/SemanticSearchServiceFactory.java`
  - Spring factory for SemanticSearchService
- `/metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/SemanticEntitySearchServiceFactory.java`
  - Spring factory for SemanticEntitySearchService
- `/metadata-service/factories/src/main/java/com/linkedin/gms/factory/search/semantic/EmbeddingProviderFactory.java`
  - Spring factory for EmbeddingProvider

## Tests

### Unit Tests

- `/metadata-io/src/test/java/com/linkedin/metadata/search/SemanticSearchServiceTest.java`
  - Unit tests for SemanticSearchService
- `/datahub-graphql-core/src/test/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchResolverTest.java`
  - Unit tests for SemanticSearchResolver
- `/datahub-graphql-core/src/test/java/com/linkedin/datahub/graphql/resolvers/semantic/SemanticSearchAcrossEntitiesResolverTest.java`
  - Unit tests for SemanticSearchAcrossEntitiesResolver

### Integration Tests

- `/metadata-io/src/test/java/com/linkedin/metadata/search/SemanticSearchServiceIT.java`
  - Integration tests for semantic search service (requires OpenSearch)
- `/metadata-io/src/test/java/com/linkedin/metadata/search/semantic/SemanticEntitySearchIT.java`
  - Integration tests for semantic entity search (requires OpenSearch)
- `/metadata-io/src/test/java/com/linkedin/metadata/search/embedding/IntegrationsServiceEmbeddingProviderIT.java`
  - Integration tests for embedding provider (requires Integrations Service)
- `/metadata-io/src/test/java/com/linkedin/metadata/search/embedding/IntegrationsServiceEmbeddingProviderIntegrationTest.java`
  - Additional integration tests for embedding provider

## Dependencies

### External Services Required

- **OpenSearch 2.17+** with k-NN plugin enabled
- **DataHub Integrations Service** for embeddings
- **AWS Bedrock Cohere** for text embedding generation

### Index Requirements

- Semantic indices with suffix `_semantic` containing vector fields
- Nested vector structure: `embeddings.cohere_embed_v3.chunks.vector`
- Vector dimensions: 1024 (Cohere Embed v3)

## Key Characteristics

All SaaS-specific files are marked with a comment header:

```java
/**
 * SAAS-SPECIFIC: This [class/interface/test] is part of the semantic search feature exclusive to DataHub SaaS.
 * It should NOT be merged back to the open-source DataHub repository.
 * Dependencies: [specific dependencies]
 */
```

## Integration Points

These SaaS components integrate with OSS DataHub at:

- `GraphQLEngineFactory` - Injection of `SemanticSearchService`
- `AcrylGraphQLPlugin` - Registration of semantic search resolvers
- Search result merging - Facets from keyword search attached to semantic results

## Exclusion from OSS Merge

When merging changes back to open-source DataHub, exclude:

1. All files listed in this document
2. Any references to `SemanticSearchService` in shared components
3. Semantic search GraphQL endpoints
4. Embedding-related configurations
