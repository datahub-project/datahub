/**
 * SAAS-SPECIFIC: This class is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Requires DataHub
 * Integrations Service and AWS Bedrock Cohere.
 */
package com.linkedin.metadata.search.embedding;

import com.linkedin.metadata.integration.IntegrationsService;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of {@link EmbeddingProvider} that delegates to the DataHub integrations service to
 * generate query embeddings. This allows centralized embedding generation and model management
 * through the Python-based integrations service.
 *
 * @see com.linkedin.metadata.integration.IntegrationsService#generateQueryEmbedding
 */
public class IntegrationsServiceEmbeddingProvider implements EmbeddingProvider {

  private static final String DEFAULT_MODEL = "cohere.embed-english-v3";
  private final IntegrationsService integrationsService;

  /**
   * Creates a new IntegrationsServiceEmbeddingProvider.
   *
   * @param integrationsService The integrations service to use for embedding generation
   */
  public IntegrationsServiceEmbeddingProvider(
      @Nonnull final IntegrationsService integrationsService) {
    this.integrationsService = Objects.requireNonNull(integrationsService);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    // Use the default model if none is specified
    String modelToUse = model != null ? model : DEFAULT_MODEL;
    return integrationsService.generateQueryEmbedding(text, modelToUse);
  }
}
