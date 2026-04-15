package com.linkedin.gms.factory.search.semantic;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.AwsBedrockEmbeddingProvider;
import com.linkedin.metadata.search.embedding.CohereEmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.embedding.OpenAIEmbeddingProvider;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for creating embedding providers used in semantic search.
 *
 * <p>Supports three embedding providers:
 *
 * <ul>
 *   <li><b>aws-bedrock</b>: AWS Bedrock Runtime API with Cohere/Titan models
 *   <li><b>openai</b>: OpenAI Embeddings API with text-embedding-3-small/large models
 *   <li><b>cohere</b>: Cohere Embed API with embed-english-v3.0/multilingual-v3.0 models
 * </ul>
 *
 * <p>The provider is conditionally created only when semantic search is enabled in the
 * configuration.
 *
 * <p>AWS Bedrock credentials are resolved automatically using the default AWS credential provider
 * chain:
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

    return switch (providerType.toLowerCase()) {
      case "aws-bedrock" -> createAwsBedrockProvider(config);
      case "openai" -> createOpenAIProvider(config);
      case "cohere" -> createCohereProvider(config);
      default -> throw new IllegalStateException(
          String.format(
              "Unsupported embedding provider type: %s. Supported types: aws-bedrock, openai, cohere",
              providerType));
    };
  }

  private EmbeddingProvider createAwsBedrockProvider(EmbeddingProviderConfiguration config) {
    EmbeddingProviderConfiguration.BedrockConfig bedrockConfig = config.getBedrock();

    log.info(
        "Configuring AWS Bedrock embedding provider: region={}, model={}, maxCharLength={}",
        bedrockConfig.getAwsRegion(),
        bedrockConfig.getModel(),
        config.getMaxCharacterLength());

    return new AwsBedrockEmbeddingProvider(
        bedrockConfig.getAwsRegion(), bedrockConfig.getModel(), config.getMaxCharacterLength());
  }

  private EmbeddingProvider createOpenAIProvider(EmbeddingProviderConfiguration config) {
    EmbeddingProviderConfiguration.OpenAIConfig openaiConfig = config.getOpenai();

    if (openaiConfig.getApiKey() == null || openaiConfig.getApiKey().isBlank()) {
      throw new IllegalStateException(
          "OpenAI API key is required when using 'openai' embedding provider. "
              + "Set the OPENAI_API_KEY environment variable or configure embeddingProvider.openai.apiKey in application.yaml");
    }

    log.info(
        "Configuring OpenAI embedding provider: endpoint={}, model={}",
        openaiConfig.getEndpoint(),
        openaiConfig.getModel());

    return new OpenAIEmbeddingProvider(
        openaiConfig.getApiKey(), openaiConfig.getEndpoint(), openaiConfig.getModel());
  }

  private EmbeddingProvider createCohereProvider(EmbeddingProviderConfiguration config) {
    EmbeddingProviderConfiguration.CohereConfig cohereConfig = config.getCohere();

    if (cohereConfig.getApiKey() == null || cohereConfig.getApiKey().isBlank()) {
      throw new IllegalStateException(
          "Cohere API key is required when using 'cohere' embedding provider. "
              + "Set the COHERE_API_KEY environment variable or configure embeddingProvider.cohere.apiKey in application.yaml");
    }

    log.info(
        "Configuring Cohere embedding provider: endpoint={}, model={}",
        cohereConfig.getEndpoint(),
        cohereConfig.getModel());

    return new CohereEmbeddingProvider(
        cohereConfig.getApiKey(), cohereConfig.getEndpoint(), cohereConfig.getModel());
  }
}
