package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

/**
 * Implementation of {@link EmbeddingProvider} that directly calls AWS Bedrock to generate query
 * embeddings using Cohere Embed models.
 *
 * <p>This provider uses credentials from a shared {@link AwsCredentialsProvider} (typically the
 * {@code defaultAwsCredentialsProvider} bean in GMS). The {@link BedrockRuntimeClient} region is
 * set independently (typically via {@code embeddingProvider.bedrock.awsRegion}), so Bedrock can be
 * called in a different region than the pod's {@code AWS_REGION} without creating additional
 * credential providers.
 *
 * <p>Supports Cohere Embed v3 models with proper input_type and truncation handling.
 *
 * @see <a
 *     href="https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed.html">Cohere
 *     Embed Models</a>
 */
@Slf4j
public class AwsBedrockEmbeddingProvider implements EmbeddingProvider {

  private static final String DEFAULT_MODEL = "cohere.embed-english-v3";
  private static final int DEFAULT_MAX_CHARACTER_LENGTH = 2048; // Cohere's hard limit

  // A call with a deadline does not retry a throttle: a retry's backoff would outlive the deadline
  // and end the throttle as a timeout. It retries a transient server or connection error once,
  // quickly, so one blip does not fail the search.
  private static final RetryStrategy DEADLINE_RETRIES =
      StandardRetryStrategy.builder()
          .maxAttempts(2)
          .backoffStrategy(BackoffStrategy.fixedDelay(Duration.ofMillis(50)))
          .retryOnException(AwsBedrockEmbeddingProvider::transientNotThrottled)
          .circuitBreakerEnabled(false)
          .build();

  // Applied per request, so a call without a deadline keeps the client's retries
  private static final SdkPlugin WITHIN_DEADLINE =
      config -> config.overrideConfiguration(o -> o.retryStrategy(DEADLINE_RETRIES));

  private final BedrockRuntimeClient bedrockClient;
  private final ObjectMapper objectMapper;
  private final String defaultModel;
  private final int maxCharacterLength;

  /**
   * Creates a provider using an injected shared credentials provider (required in GMS).
   *
   * @param awsRegion AWS region where Bedrock is available (e.g., "us-west-2", "us-east-1")
   * @param credentialsProvider shared process-wide provider from {@code AwsClientFactory}
   */
  public AwsBedrockEmbeddingProvider(
      @Nonnull String awsRegion, @Nonnull AwsCredentialsProvider credentialsProvider) {
    this(awsRegion, DEFAULT_MODEL, DEFAULT_MAX_CHARACTER_LENGTH, credentialsProvider);
  }

  /**
   * Creates a provider using an injected shared credentials provider (required in GMS).
   *
   * @param credentialsProvider shared process-wide provider from {@code AwsClientFactory}
   */
  public AwsBedrockEmbeddingProvider(
      @Nonnull String awsRegion,
      @Nonnull String defaultModel,
      int maxCharacterLength,
      @Nonnull AwsCredentialsProvider credentialsProvider) {
    Objects.requireNonNull(awsRegion, "awsRegion cannot be null");
    Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    Objects.requireNonNull(credentialsProvider, "credentialsProvider cannot be null");

    this.defaultModel = defaultModel;
    this.maxCharacterLength = maxCharacterLength;
    this.objectMapper = new ObjectMapper();

    this.bedrockClient =
        BedrockRuntimeClient.builder()
            .region(Region.of(awsRegion))
            .credentialsProvider(credentialsProvider)
            .build();

    log.info(
        "Initialized AwsBedrockEmbeddingProvider with region={}, model={}, maxCharLength={}",
        awsRegion,
        defaultModel,
        maxCharacterLength);
  }

  /**
   * Creates a provider with an existing BedrockRuntimeClient (useful for testing or custom client
   * configuration).
   *
   * @param bedrockClient Pre-configured Bedrock Runtime client
   * @param defaultModel Default embedding model
   * @param maxCharacterLength Maximum text length before truncation
   */
  public AwsBedrockEmbeddingProvider(
      @Nonnull BedrockRuntimeClient bedrockClient,
      @Nonnull String defaultModel,
      int maxCharacterLength) {
    this.bedrockClient = Objects.requireNonNull(bedrockClient, "bedrockClient cannot be null");
    this.defaultModel = Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    this.maxCharacterLength = maxCharacterLength;
    this.objectMapper = new ObjectMapper();

    log.info(
        "Initialized AwsBedrockEmbeddingProvider with custom client, model={}, maxCharLength={}",
        defaultModel,
        maxCharacterLength);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    return embedWithTimeout(text, model, null);
  }

  @Override
  @Nonnull
  public float[] embed(
      @Nonnull String text,
      @Nullable String model,
      @Nonnull EmbeddingTaskType taskType,
      @Nonnull Duration timeout) {
    return embedWithTimeout(text, model, timeout);
  }

  /**
   * @param timeout bounds the call, which then retries only a transient error, once; null keeps the
   *     client's settings, retries included
   */
  @Nonnull
  private float[] embedWithTimeout(
      @Nonnull String text, @Nullable String model, @Nullable Duration timeout) {
    Objects.requireNonNull(text, "text cannot be null");

    String modelToUse = model != null ? model : defaultModel;

    // Truncate text if it exceeds the max character length
    // Cohere enforces a 2048-character limit separate from token context window
    String truncatedText = text;
    if (text.length() > maxCharacterLength) {
      truncatedText = text.substring(0, maxCharacterLength);
      log.info("Truncated input text from {} to {} characters", text.length(), maxCharacterLength);
    }

    try {
      // Build request JSON for Cohere Embed v3
      // Format: {"texts": ["text"], "input_type": "search_query", "truncate": "END"}
      ObjectNode requestBody = objectMapper.createObjectNode();

      // texts: array with single text (we're embedding one query at a time)
      ArrayNode textsArray = objectMapper.createArrayNode();
      textsArray.add(truncatedText);
      requestBody.set("texts", textsArray);

      // input_type: "search_query" for query embeddings (required for Cohere v3)
      requestBody.put("input_type", "search_query");

      // truncate: "END" to enable auto-truncation for token limits
      requestBody.put("truncate", "END");

      String requestJson = objectMapper.writeValueAsString(requestBody);
      log.debug("Bedrock request for model {}: {}", modelToUse, requestJson);

      // Invoke Bedrock model
      InvokeModelRequest.Builder invokeRequest =
          InvokeModelRequest.builder()
              .modelId(modelToUse)
              .body(SdkBytes.fromString(requestJson, StandardCharsets.UTF_8))
              .contentType("application/json")
              .accept("application/json");
      if (timeout != null) {
        invokeRequest.overrideConfiguration(
            override -> override.apiCallTimeout(timeout).addPlugin(WITHIN_DEADLINE));
      }

      InvokeModelResponse response = bedrockClient.invokeModel(invokeRequest.build());

      // Parse response
      // Format: {"embeddings": [[0.123, 0.456, ...]], "id": "...", "response_type":
      // "embeddings_floats", "texts": ["..."]}
      String responseJson = response.body().asString(StandardCharsets.UTF_8);
      log.debug("Bedrock response: {}", responseJson);

      JsonNode responseNode = objectMapper.readTree(responseJson);
      JsonNode embeddingsNode = responseNode.get("embeddings");

      if (embeddingsNode == null || !embeddingsNode.isArray() || embeddingsNode.size() == 0) {
        throw new RuntimeException(
            "Invalid response from Bedrock: missing or empty embeddings array");
      }

      // Extract first (and only) embedding
      JsonNode embeddingArray = embeddingsNode.get(0);
      if (!embeddingArray.isArray()) {
        throw new RuntimeException("Invalid response from Bedrock: embedding is not an array");
      }

      // Convert to float[]
      int dimensions = embeddingArray.size();
      float[] embedding = new float[dimensions];
      for (int i = 0; i < dimensions; i++) {
        JsonNode value = embeddingArray.get(i);
        if (value.isNumber()) {
          embedding[i] = (float) value.asDouble();
        } else {
          throw new RuntimeException(
              "Invalid response from Bedrock: embedding contains non-numeric value");
        }
      }

      log.debug("Generated embedding with {} dimensions for model {}", dimensions, modelToUse);
      return embedding;

    } catch (IOException e) {
      String errorMsg =
          String.format(
              "Failed to generate embedding with model %s: %s", modelToUse, e.getMessage());
      log.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    } catch (Exception e) {
      String errorMsg =
          String.format("Bedrock InvokeModel failed for model %s: %s", modelToUse, e.getMessage());
      // Any 429, which includes ModelNotReadyException as well as ThrottlingException: both clear
      // on their own, so reporting them as a throttle, a 503 to GraphQL callers, is intended. With
      // no deadline the client's retries ran first.
      if (e instanceof SdkServiceException service && service.isThrottlingException()) {
        log.warn(errorMsg);
        throw new EmbeddingThrottledException(errorMsg, e);
      }
      log.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /** A transient server error or a dropped connection, never a throttle. */
  private static boolean transientNotThrottled(@Nonnull Throwable failure) {
    if (failure instanceof SdkServiceException service) {
      return !service.isThrottlingException() && service.statusCode() >= 500;
    }
    return failure instanceof SdkClientException && failure.getCause() instanceof IOException;
  }

  /** Closes the Bedrock client. Should be called when the provider is no longer needed. */
  public void close() {
    if (bedrockClient != null) {
      bedrockClient.close();
      log.info("Closed BedrockRuntimeClient");
    }
  }
}
