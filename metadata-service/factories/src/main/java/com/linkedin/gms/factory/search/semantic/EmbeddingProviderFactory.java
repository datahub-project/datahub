package com.linkedin.gms.factory.search.semantic;

import com.google.auth.oauth2.GoogleCredentials;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.AwsBedrockEmbeddingProvider;
import com.linkedin.metadata.search.embedding.CohereEmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.LocalEmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.embedding.OpenAIEmbeddingProvider;
import com.linkedin.metadata.search.embedding.VertexAiEmbeddingProvider;
import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for creating embedding providers used in semantic search.
 *
 * <p>Supports multiple embedding provider types:
 *
 * <ul>
 *   <li><b>aws-bedrock</b>: AWS Bedrock Runtime API with Cohere/Titan models
 *   <li><b>openai</b>: OpenAI Embeddings API with text-embedding-3-small/large models
 *   <li><b>cohere</b>: Cohere Embed API with embed-english-v3.0/multilingual-v3.0 models
 *   <li><b>local</b>: Any locally-running OpenAI-compatible server (Ollama, LM Studio, etc.)
 *   <li><b>vertex_ai</b>: Google Vertex AI Embeddings API with Gemini embedding models
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
      case "local" -> createLocalProvider(config);
      case "vertex_ai" -> createVertexAiProvider(config);
      default -> throw new IllegalStateException(
          String.format(
              "Unsupported embedding provider type: %s. Supported types: aws-bedrock, openai, cohere, local, vertex_ai",
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

  private EmbeddingProvider createLocalProvider(EmbeddingProviderConfiguration config) {
    EmbeddingProviderConfiguration.LocalConfig localConfig = config.getLocal();

    log.info(
        "Configuring local embedding provider: endpoint={}, model={}",
        localConfig.getEndpoint(),
        localConfig.getModel());

    return new LocalEmbeddingProvider(localConfig.getEndpoint(), localConfig.getModel());
  }

  EmbeddingProvider createVertexAiProvider(EmbeddingProviderConfiguration config) {
    EmbeddingProviderConfiguration.VertexAiConfig v = config.getVertexai();

    if (v == null || v.getProjectId() == null || v.getProjectId().isBlank()) {
      throw new IllegalStateException(
          "vertex_ai embedding provider requires projectId. "
              + "Set the VERTEX_AI_PROJECT_ID environment variable or configure embeddingProvider.vertexai.projectId in application.yaml");
    }

    if (v.getLocation() == null || v.getLocation().isBlank()) {
      throw new IllegalStateException(
          "vertex_ai embedding provider requires location. "
              + "Set the VERTEX_AI_LOCATION environment variable or configure embeddingProvider.vertexai.location in application.yaml");
    }

    String model =
        v.getModel() != null && !v.getModel().isBlank() ? v.getModel() : "gemini-embedding-001";
    // outputDimensionality: 0 means "use model native"; for gemini-embedding-001 native is 768.
    // Until the ingestion-side client supports propagating non-default dimensions
    // (https://github.com/datahub-project/datahub/pull/17255), reject any non-default value
    // to prevent server/client embedding-dimension mismatch that would break kNN search.
    int dims = v.getOutputDimensionality() > 0 ? v.getOutputDimensionality() : 768;
    if (dims != 768) {
      throw new IllegalStateException(
          "Vertex AI outputDimensionality="
              + dims
              + " is not currently supported. Only the default (768, gemini-embedding-001 native) "
              + "is allowed because the ingestion-side client does not yet propagate this setting. "
              + "Mismatched dimensions between search and ingestion would break kNN search. "
              + "Either remove VERTEX_AI_EMBEDDING_OUTPUT_DIMENSIONALITY or set it to 768.");
    }

    log.info(
        "Configuring Vertex AI embedding provider: project={}, location={}, model={}, dims={}",
        v.getProjectId(),
        v.getLocation(),
        model,
        dims);

    Supplier<String> tokenSupplier = buildVertexAiTokenSupplier();

    return new VertexAiEmbeddingProvider(
        v.getProjectId(), v.getLocation(), model, dims, tokenSupplier);
  }

  /**
   * Builds the GCP token supplier used by the Vertex AI embedding provider.
   *
   * <p>Credentials are resolved once via Application Default Credentials, scoped to the Cloud
   * Platform API, and then reused across calls. {@link GoogleCredentials#refreshIfExpired()} is
   * used on each invocation so that tokens are only refreshed when stale — not on every embed call.
   * The eager {@code refreshIfExpired()} call at construction time validates the credentials at
   * startup rather than on the first search request, surfacing misconfiguration early.
   *
   * <p>Protected to allow override in tests without a live GCP environment.
   */
  protected Supplier<String> buildVertexAiTokenSupplier() {
    final GoogleCredentials credentials;
    try {
      credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped("https://www.googleapis.com/auth/cloud-platform");
      // Fail fast: validate credentials at startup rather than on the first search request.
      credentials.refreshIfExpired();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to initialise GCP credentials for Vertex AI", e);
    }

    return () -> {
      try {
        credentials.refreshIfExpired();
        com.google.auth.oauth2.AccessToken token = credentials.getAccessToken();
        if (token == null || token.getTokenValue() == null) {
          throw new RuntimeException(
              "GCP credentials returned a null access token after refresh. "
                  + "Check that Application Default Credentials are configured correctly.");
        }
        return token.getTokenValue();
      } catch (IOException e) {
        throw new RuntimeException("Failed to obtain GCP access token", e);
      }
    };
  }
}
