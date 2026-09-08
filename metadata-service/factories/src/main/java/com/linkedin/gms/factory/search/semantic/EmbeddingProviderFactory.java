package com.linkedin.gms.factory.search.semantic;

import com.google.auth.oauth2.GoogleCredentials;
import com.linkedin.gms.factory.aws.AwsClientFactory;
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
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

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
 * <p>AWS Bedrock uses the shared {@code defaultAwsCredentialsProvider} bean from {@link
 * AwsClientFactory}. The Bedrock Runtime client region comes from {@code
 * embeddingProvider.bedrock.awsRegion} and may differ from the pod {@code AWS_REGION} for
 * cross-region model access.
 */
@Slf4j
@Configuration
@Import(AwsClientFactory.class)
public class EmbeddingProviderFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  @Qualifier("defaultAwsCredentialsProvider")
  private AwsCredentialsProvider defaultAwsCredentialsProvider;

  @Nullable private AwsBedrockEmbeddingProvider managedBedrockProvider;

  /**
   * Creates an EmbeddingProvider bean for generating query embeddings.
   *
   * <p>Returns a no-op provider if semantic search is not enabled, allowing the system to start
   * without requiring embedding configuration.
   *
   * <p>{@code destroyMethod} is disabled so this factory's {@link PreDestroy} is the sole closer of
   * the Bedrock Runtime client (avoids double-close with Spring's inferred {@code Closeable}
   * destroy).
   *
   * @return EmbeddingProvider instance configured based on application.yaml settings
   */
  @Bean(name = "embeddingProvider", destroyMethod = "")
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
      default ->
          throw new IllegalStateException(
              String.format(
                  "Unsupported embedding provider type: %s. Supported types: aws-bedrock, openai, cohere, local, vertex_ai",
                  providerType));
    };
  }

  private EmbeddingProvider createAwsBedrockProvider(EmbeddingProviderConfiguration config) {
    if (defaultAwsCredentialsProvider == null) {
      throw new IllegalStateException(
          "Shared DefaultCredentialsProvider is required when using the aws-bedrock embedding provider. "
              + "Configure AWS_REGION/aws.region/AWS_ENDPOINT_URL on the pod, or enable aws-bedrock with bedrock.awsRegion set.");
    }

    EmbeddingProviderConfiguration.BedrockConfig bedrockConfig = config.getBedrock();
    String bedrockRegion = bedrockConfig.getAwsRegion();
    if (bedrockRegion == null || bedrockRegion.trim().isEmpty()) {
      throw new IllegalStateException(
          "embeddingProvider.bedrock.awsRegion is required when using the aws-bedrock embedding provider");
    }
    bedrockRegion = bedrockRegion.trim();

    String podRegion = System.getenv("AWS_REGION");
    if (podRegion == null || podRegion.isBlank()) {
      podRegion = System.getProperty("AWS_REGION");
    }
    if (podRegion != null
        && !podRegion.isBlank()
        && !podRegion.trim().equalsIgnoreCase(bedrockRegion)) {
      log.info(
          "Bedrock embedding region {} differs from pod AWS_REGION {}; using shared credentials with cross-region Bedrock client",
          bedrockRegion,
          podRegion.trim());
    }

    log.info(
        "Configuring AWS Bedrock embedding provider: bedrockRegion={}, model={}, maxCharLength={}",
        bedrockRegion,
        bedrockConfig.getModel(),
        config.getMaxCharacterLength());

    managedBedrockProvider =
        new AwsBedrockEmbeddingProvider(
            bedrockRegion,
            bedrockConfig.getModel(),
            config.getMaxCharacterLength(),
            defaultAwsCredentialsProvider);
    return managedBedrockProvider;
  }

  @PreDestroy
  public void shutdown() {
    if (managedBedrockProvider != null) {
      managedBedrockProvider.close();
      managedBedrockProvider = null;
    }
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
    // outputDimensionality: 0 means "use model native"; for gemini-embedding-001 native is 3072.
    int dims = v.getOutputDimensionality() > 0 ? v.getOutputDimensionality() : 3072;

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
