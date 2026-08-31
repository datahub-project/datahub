package com.linkedin.gms.factory.search.semantic;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SemanticEntitySearchServiceFactory {

  private static final String DEFAULT_MODEL_EMBEDDING_KEY = "text_embedding_3_large";

  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Autowired
  @Qualifier("embeddingProvider")
  private EmbeddingProvider embeddingProvider;

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "semanticEntitySearchService")
  @Nonnull
  protected SemanticEntitySearch getInstance(
      @Qualifier("mappingsBuilder") final MappingsBuilder mappingsBuilder) {

    String modelEmbeddingKey = deriveModelEmbeddingKey();
    log.info("Creating SemanticEntitySearchService with modelEmbeddingKey={}", modelEmbeddingKey);

    return new SemanticEntitySearchService(
        searchClient, embeddingProvider, mappingsBuilder, modelEmbeddingKey);
  }

  /**
   * Derive the model embedding key from the configured embedding provider.
   *
   * <p>This follows the same logic as AppConfigResolver.deriveModelEmbeddingKey() to ensure
   * consistency between the server-side semantic search and the client-side ingestion.
   */
  private String deriveModelEmbeddingKey() {
    SemanticSearchConfiguration semanticSearchConfig =
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch();

    if (semanticSearchConfig == null || !semanticSearchConfig.isEnabled()) {
      log.warn(
          "Semantic search not configured, using default modelEmbeddingKey={}",
          DEFAULT_MODEL_EMBEDDING_KEY);
      return DEFAULT_MODEL_EMBEDDING_KEY;
    }

    EmbeddingProviderConfiguration config = semanticSearchConfig.getEmbeddingProvider();
    if (config == null) {
      log.warn(
          "Embedding provider not configured, using default modelEmbeddingKey={}",
          DEFAULT_MODEL_EMBEDDING_KEY);
      return DEFAULT_MODEL_EMBEDDING_KEY;
    }

    String modelId = config.getModelId();

    String key = deriveModelEmbeddingKeyFromModelId(modelId);
    log.info(
        "Derived modelEmbeddingKey={} from providerType={}, modelId={}",
        key,
        config.getType(),
        modelId);
    return key;
  }

  /**
   * Derive canonical model embedding key from model ID for use in SemanticContent aspects.
   *
   * <p>This is the single source of truth for modelEmbeddingKey derivation. Clients must use the
   * modelEmbeddingKey provided by this method to ensure consistency between client (writing
   * embeddings) and server (querying embeddings).
   *
   * <p>The modelEmbeddingKey is used as the key in the SemanticContent embeddings map and as the
   * field name in Elasticsearch indices.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>cohere.embed-english-v3 → cohere_embed_v3
   *   <li>cohere.embed-multilingual-v3 → cohere_embed_multilingual_v3
   *   <li>amazon.titan-embed-text-v1 → amazon_titan_v1
   *   <li>text-embedding-3-small → text_embedding_3_small
   *   <li>text-embedding-3-large → text_embedding_3_large
   *   <li>embed-english-v3.0 → embed_english_v3_0
   * </ul>
   */
  private static String deriveModelEmbeddingKeyFromModelId(final String modelId) {
    if (modelId == null || modelId.isBlank()) {
      return DEFAULT_MODEL_EMBEDDING_KEY;
    }

    // Cohere native models (embed-english-v3.0, embed-multilingual-v3.0)
    // Check these FIRST because they also match the "embed-english-v3" pattern
    if (modelId.contains("embed-english-v3.0")) return "embed_english_v3_0";
    if (modelId.contains("embed-multilingual-v3.0")) return "embed_multilingual_v3_0";
    if (modelId.contains("embed-english-light-v3.0")) return "embed_english_light_v3_0";
    // AWS Bedrock Cohere models (without the .0 suffix)
    if (modelId.contains("embed-english-v3")) return "cohere_embed_v3";
    if (modelId.contains("embed-multilingual-v3")) return "cohere_embed_multilingual_v3";
    // AWS Bedrock Titan models
    if (modelId.contains("titan-embed-text-v1")) return "amazon_titan_v1";
    if (modelId.contains("titan-embed-text-v2")) return "amazon_titan_v2";

    // Fallback: replace special chars with underscores
    return modelId.replace("-", "_").replace(".", "_").replace(":", "_");
  }
}
