package com.linkedin.gms.factory.search.semantic;

import com.google.auth.oauth2.GoogleCredentials;
import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.AwsBedrockEmbeddingProvider;
import com.linkedin.metadata.search.embedding.ClassicalEmbeddingProvider;
import com.linkedin.metadata.search.embedding.CohereEmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.LocalEmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.embedding.OnnxEmbeddingProvider;
import com.linkedin.metadata.search.embedding.OpenAIEmbeddingProvider;
import com.linkedin.metadata.search.embedding.VertexAiEmbeddingProvider;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
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
 *   <li><b>ai-gateway</b>: OAuth2-authenticated AI gateway embedding API (fronts Vertex AI, Azure
 *       OpenAI, and Bedrock behind a single contract)
 *   <li><b>local</b>: Any locally-running OpenAI-compatible server (Ollama, LM Studio, etc.)
 *   <li><b>vertex_ai</b>: Google Vertex AI Embeddings API with Gemini embedding models
 *   <li><b>onnx</b>: In-process ONNX Runtime inference (no external server required)
 *   <li><b>classical</b>: Deterministic in-process lexical hashing (no external service or model
 *       files)
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

  @Nullable private OnnxEmbeddingProvider managedOnnxProvider;

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

    return switch (providerType.toLowerCase(Locale.ROOT)) {
      case "aws-bedrock" -> createAwsBedrockProvider(config);
      case "openai" -> createOpenAIProvider(config);
      case "cohere" -> createCohereProvider(config);
      case "local" -> createLocalProvider(config);
      case "vertex_ai" -> createVertexAiProvider(config);
      case "onnx" -> createOnnxProvider(config, semanticSearchConfig);
      case "classical" -> createClassicalProvider(config, semanticSearchConfig);
      case "ai-gateway" -> createAiGatewayProvider(config);
      default ->
          throw new IllegalStateException(
              String.format(
                  "Unsupported embedding provider type: %s. Supported types: aws-bedrock, openai, cohere, local, vertex_ai, onnx, classical, ai-gateway",
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
    // ONNX holds native OrtSession + tokenizer allocations. destroyMethod is disabled on the bean,
    // so Spring will not auto-close it — release here on context shutdown to avoid a native leak.
    if (managedOnnxProvider != null) {
      managedOnnxProvider.close();
      managedOnnxProvider = null;
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

  private EmbeddingProvider createOnnxProvider(
      EmbeddingProviderConfiguration config, SemanticSearchConfiguration semanticSearchConfig) {
    EmbeddingProviderConfiguration.OnnxConfig onnxConfig = config.getOnnx();
    if (onnxConfig == null) {
      throw new IllegalStateException(
          "ONNX configuration block is missing. "
              + "Configure embeddingProvider.onnx in application.yaml with modelName and modelDir.");
    }

    if (onnxConfig.getModelName() == null || onnxConfig.getModelName().isBlank()) {
      throw new IllegalStateException(
          "ONNX model name is required when using 'onnx' embedding provider. "
              + "Set the ONNX_EMBEDDING_MODEL_NAME environment variable or configure "
              + "embeddingProvider.onnx.modelName in application.yaml. "
              + "This must match a key in semanticSearch.models (e.g., 'snowflake_arctic_embed_s').");
    }

    if (onnxConfig.getModelDir() == null || onnxConfig.getModelDir().isBlank()) {
      throw new IllegalStateException(
          "ONNX model directory is required when using 'onnx' embedding provider. "
              + "Set the ONNX_EMBEDDING_MODEL_DIR environment variable or configure "
              + "embeddingProvider.onnx.modelDir in application.yaml");
    }

    String modelName = onnxConfig.getModelName();

    // Validate that the model name matches a key in the models map
    Map<String, ModelEmbeddingConfig> models = semanticSearchConfig.getModels();
    if (models == null || !models.containsKey(modelName)) {
      String available = models != null ? models.keySet().toString() : "none";
      throw new IllegalStateException(
          String.format(
              "ONNX model name '%s' does not match any entry in semanticSearch.models. "
                  + "Available keys: %s. Add an entry for '%s' with the correct vectorDimension "
                  + "in application.yaml or set the corresponding environment variables.",
              modelName, available, modelName));
    }

    Path modelDir = Path.of(onnxConfig.getModelDir());
    log.info(
        "Configuring ONNX embedding provider: modelName={}, modelDir={}, intraOpThreads={}",
        modelName,
        modelDir,
        onnxConfig.getIntraOpThreads());

    OnnxEmbeddingProvider provider =
        new OnnxEmbeddingProvider(
            modelDir,
            onnxConfig.getIntraOpThreads(),
            config.getMaxCharacterLength(),
            onnxConfig.getPooling(),
            onnxConfig.getQueryInstruction());

    EmbeddingProvider validated = validateOnnxProviderDimension(provider, models, modelName);
    // Track only after validation passes; on mismatch validateOnnxProviderDimension has already
    // closed the provider and thrown, so managedOnnxProvider must stay null (no double-close).
    managedOnnxProvider = provider;
    return validated;
  }

  /**
   * Validates the loaded ONNX model's actual output dimension against the configured {@code
   * vectorDimension} and returns the provider on match. On mismatch (or any failure) the provider
   * is closed to release native resources before the exception propagates. Package-private so the
   * dimension-check logic can be unit-tested with a mocked provider (real model loading requires a
   * model file on disk).
   */
  EmbeddingProvider validateOnnxProviderDimension(
      OnnxEmbeddingProvider provider, Map<String, ModelEmbeddingConfig> models, String modelName) {
    try {
      int actualDim = provider.getOutputDimension();
      int configuredDim = models.get(modelName).getVectorDimension();
      if (actualDim != configuredDim) {
        throw new IllegalStateException(
            String.format(
                "ONNX model output dimension (%d) does not match the configured vectorDimension (%d) "
                    + "for model '%s' in semanticSearch.models. Either change the model or set "
                    + "semanticSearch.models.%s.vectorDimension=%d in application.yaml (requires reindexing if changed).",
                actualDim, configuredDim, modelName, modelName, actualDim));
      }

      log.info(
          "ONNX dimension validation passed: model '{}' produces {}-dim embeddings, "
              + "matching configured vectorDimension={}",
          modelName,
          actualDim,
          configuredDim);

      return provider;
    } catch (Exception e) {
      try {
        provider.close();
      } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
      }
      throw e;
    }
  }

  /**
   * Creates the classical provider and fails startup unless the {@code semanticSearch.models} entry
   * derived from the model name matches its width and uses a cosine space type: the vectors are
   * unnormalized integer counts, and a width or metric mismatch would only surface as silent empty
   * search results.
   */
  private EmbeddingProvider createClassicalProvider(
      EmbeddingProviderConfiguration config, SemanticSearchConfiguration semanticSearchConfig) {
    EmbeddingProviderConfiguration.ClassicalConfig classicalConfig = config.getClassical();
    String model = classicalConfig != null ? classicalConfig.getModel() : null;
    if (model == null || model.isBlank()) {
      throw new IllegalStateException(
          "Classical embedding model is required when using 'classical' embedding provider. "
              + "Set the CLASSICAL_EMBEDDING_MODEL environment variable or configure "
              + "embeddingProvider.classical.model in application.yaml (e.g. 'hash-v1-2048').");
    }
    // Hard gate: the provider ranks by hashed lexical overlap, not meaning, and exists for CI,
    // smoke tests and quickstarts. Document vectors are only searchable through a GMS running
    // the same provider, so refusing here scopes the whole pipeline, not just the query side.
    if (!classicalConfig.isAcknowledgeLexicalOnly()) {
      throw new IllegalStateException(
          "The classical embedding provider ranks by hashed lexical overlap, not meaning, and is "
              + "meant for CI, smoke tests and quickstarts, not for deployments that need semantic "
              + "search. To run it anyway set CLASSICAL_EMBEDDING_ACKNOWLEDGE_LEXICAL_ONLY=true "
              + "(embeddingProvider.classical.acknowledgeLexicalOnly). For a local neural provider "
              + "without an API key use EMBEDDING_PROVIDER_TYPE=onnx.");
    }

    ClassicalEmbeddingProvider provider;
    try {
      provider = new ClassicalEmbeddingProvider(model);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Invalid classical embedding model '" + model + "': " + e.getMessage(), e);
    }

    // The key the query path reads from the index; derived by the same helper so startup
    // validation and index lookup cannot drift apart.
    String modelKey = SemanticEntitySearchServiceFactory.deriveModelEmbeddingKeyFromModelId(model);
    Map<String, ModelEmbeddingConfig> models = semanticSearchConfig.getModels();
    ModelEmbeddingConfig modelConfig = models != null ? models.get(modelKey) : null;
    if (modelConfig == null) {
      String available = models != null ? models.keySet().toString() : "none";
      throw new IllegalStateException(
          String.format(
              "Classical embedding model '%s' has no entry '%s' in semanticSearch.models. "
                  + "Available keys: %s. Add an entry with vectorDimension=%d and "
                  + "spaceType=cosinesimil in application.yaml (the next system-update run "
                  + "adds the mapping to the semantic index).",
              model, modelKey, available, provider.getDimensions()));
    }
    if (modelConfig.getVectorDimension() != provider.getDimensions()) {
      throw new IllegalStateException(
          String.format(
              "Classical embedding model '%s' produces %d-dim vectors but "
                  + "semanticSearch.models.%s.vectorDimension is %d. Either change the model name "
                  + "or set vectorDimension=%d (requires reindexing if changed).",
              model,
              provider.getDimensions(),
              modelKey,
              modelConfig.getVectorDimension(),
              provider.getDimensions()));
    }
    String spaceType = modelConfig.getSpaceType();
    // Exact match: the index mapping translator does not normalize case, so a value that only
    // passes case-insensitively would fail later at semantic index creation.
    if (!"cosinesimil".equals(spaceType) && !"cosine".equals(spaceType)) {
      throw new IllegalStateException(
          String.format(
              "Classical embedding vectors are unnormalized and require a cosine space type, but "
                  + "semanticSearch.models.%s.spaceType is '%s'. Set it to exactly cosinesimil "
                  + "(OpenSearch) or cosine (Elasticsearch).",
              modelKey, spaceType));
    }

    log.warn(
        "Classical embedding provider active (model={}, modelKey={}, dimensions={}): ranking is "
            + "lexical (hashed words and character n-grams), not semantic. Intended for CI, smoke "
            + "tests and quickstarts; use a neural provider such as onnx for real deployments.",
        model,
        modelKey,
        provider.getDimensions());
    return provider;
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
   * private EmbeddingProvider createAiGatewayProvider(EmbeddingProviderConfiguration config) {
   * EmbeddingProviderConfiguration.AiGatewayConfig aiGatewayConfig = config.getAiGateway();
   *
   * <p>if (aiGatewayConfig == null || aiGatewayConfig.getBaseUrl() == null ||
   * aiGatewayConfig.getBaseUrl().isBlank()) { throw new IllegalStateException(
   * "embeddingProvider.aiGateway.baseUrl is required when using the ai-gateway embedding
   * provider"); } if (aiGatewayConfig.getPlatform() == null ||
   * aiGatewayConfig.getPlatform().isBlank()) { throw new IllegalStateException(
   * "embeddingProvider.aiGateway.platform is required when using the ai-gateway embedding
   * provider"); } if (aiGatewayConfig.getModel() == null || aiGatewayConfig.getModel().isBlank()) {
   * throw new IllegalStateException( "embeddingProvider.aiGateway.model is required when using the
   * ai-gateway embedding provider"); } if (aiGatewayConfig.getTokenUrl() == null ||
   * aiGatewayConfig.getTokenUrl().isBlank()) { throw new IllegalStateException(
   * "embeddingProvider.aiGateway.tokenUrl is required when using the ai-gateway embedding
   * provider"); } if (aiGatewayConfig.getClientId() == null ||
   * aiGatewayConfig.getClientId().isBlank()) { throw new IllegalStateException(
   * "embeddingProvider.aiGateway.clientId is required when using the ai-gateway embedding
   * provider"); } if (aiGatewayConfig.getClientSecret() == null ||
   * aiGatewayConfig.getClientSecret().isBlank()) { throw new IllegalStateException( "AI Gateway
   * client secret is required when using the ai-gateway embedding provider. " + "Set the
   * AI_GATEWAY_CLIENT_SECRET environment variable or configure
   * embeddingProvider.aiGateway.clientSecret in application.yaml"); }
   *
   * <p>log.info( "Configuring AI Gateway embedding provider: baseUrl={}, platform={}, model={},
   * dimensions={}", aiGatewayConfig.getBaseUrl(), aiGatewayConfig.getPlatform(),
   * aiGatewayConfig.getModel(), aiGatewayConfig.getDimensions());
   *
   * <p>return new AiGatewayEmbeddingProvider( aiGatewayConfig.getBaseUrl(),
   * aiGatewayConfig.getPlatform(), aiGatewayConfig.getModel(), aiGatewayConfig.getTokenUrl(),
   * aiGatewayConfig.getClientId(), aiGatewayConfig.getClientSecret(),
   * aiGatewayConfig.getDimensions()); }
   *
   * <p>The eager {@code refreshIfExpired()} call at construction time validates the credentials at
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
