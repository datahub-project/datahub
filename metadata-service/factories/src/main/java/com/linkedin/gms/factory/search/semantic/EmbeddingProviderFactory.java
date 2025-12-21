package com.linkedin.gms.factory.search.semantic;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.AwsBedrockEmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for creating embedding providers used in semantic search.
 *
 * <p>Currently supports AWS Bedrock with Cohere Embed models. The provider is conditionally created
 * only when semantic search is enabled in the configuration.
 *
 * <p>AWS credentials are resolved automatically using the default AWS credential provider chain:
 *
 * <ul>
 *   <li>Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 *   <li>AWS_PROFILE environment variable (reads from ~/.aws/credentials)
 *   <li>EC2 instance profile credentials (for production deployments)
 *   <li>Container credentials (ECS tasks)
 * </ul>
 */
@Slf4j
@Configuration
public class EmbeddingProviderFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  /**
   * Creates an EmbeddingProvider bean for generating query embeddings.
   *
   * <p>Returns a no-op provider if semantic search is not enabled, allowing the system to start
   * without requiring embedding configuration.
   *
   * @return EmbeddingProvider instance configured based on application.yaml settings
   */
  @Bean(name = "embeddingProvider")
  @Nonnull
  protected EmbeddingProvider getInstance() {
    SemanticSearchConfiguration semanticSearchConfig =
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch();

    if (semanticSearchConfig == null || !semanticSearchConfig.isEnabled()) {
      log.info(
          "Semantic search is not configured or not enabled. Using no-op embedding provider that will throw exceptions if used.");
      return new NoOpEmbeddingProvider();
    }

    EmbeddingProviderConfiguration config = semanticSearchConfig.getEmbeddingProvider();

    String providerType = config.getType();
    log.info("Creating embedding provider with type: {}", providerType);

    if ("aws-bedrock".equalsIgnoreCase(providerType)) {
      log.info(
          "Configuring AWS Bedrock embedding provider: region={}, model={}, maxCharLength={}",
          config.getAwsRegion(),
          config.getModelId(),
          config.getMaxCharacterLength());

      return new AwsBedrockEmbeddingProvider(
          config.getAwsRegion(), config.getModelId(), config.getMaxCharacterLength());
    } else {
      throw new IllegalStateException(
          String.format(
              "Unsupported embedding provider type: %s. Only 'aws-bedrock' is currently supported.",
              providerType));
    }
  }
}
