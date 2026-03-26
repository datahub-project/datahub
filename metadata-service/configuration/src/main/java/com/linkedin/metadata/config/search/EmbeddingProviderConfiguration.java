package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for embedding providers used to generate query embeddings for semantic search.
 *
 * <p>Supports three providers:
 *
 * <ul>
 *   <li><b>aws-bedrock</b>: AWS Bedrock Runtime API with Cohere/Titan models
 *   <li><b>openai</b>: OpenAI Embeddings API with text-embedding-3-small/large/ada-002 models
 *   <li><b>cohere</b>: Cohere Embed API with embed-english-v3.0/multilingual-v3.0 models
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmbeddingProviderConfiguration {

  /**
   * Type of embedding provider. Supported values: "openai", "aws-bedrock", "cohere". Defaults to
   * "openai".
   */
  private String type = "openai";

  /**
   * Maximum text length in characters before truncation. Cohere Embed v3 enforces a 2048-character
   * limit on the request body separate from the token context window. Defaults to 2048.
   */
  private int maxCharacterLength = 2048;

  /** Configuration for AWS Bedrock embedding provider. */
  private BedrockConfig bedrock = new BedrockConfig();

  /** Configuration for OpenAI embedding provider. */
  private OpenAIConfig openai = new OpenAIConfig();

  /** Configuration for Cohere embedding provider. */
  private CohereConfig cohere = new CohereConfig();

  /**
   * Returns the model ID for the configured provider type, pulling from the appropriate sub-config.
   */
  public String getModelId() {
    if (type == null) {
      return null;
    }
    switch (type.toLowerCase()) {
      case "openai":
        return openai != null ? openai.getModel() : null;
      case "cohere":
        return cohere != null ? cohere.getModel() : null;
      case "aws-bedrock":
        return bedrock != null ? bedrock.getModel() : null;
      default:
        return null;
    }
  }

  /** AWS Bedrock-specific configuration. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class BedrockConfig {
    /**
     * AWS region where Bedrock is available (e.g., "us-west-2", "us-east-1"). Required for
     * aws-bedrock provider.
     */
    private String awsRegion = "us-west-2";

    /**
     * Bedrock model ID for embeddings. Defaults to "cohere.embed-english-v3" (1024 dimensions).
     * Other options: - "cohere.embed-multilingual-v3" (1024 dimensions) -
     * "amazon.titan-embed-text-v1" (1536 dimensions) - "amazon.titan-embed-text-v2:0" (1024
     * dimensions default)
     */
    private String model = "cohere.embed-english-v3";
  }

  /** OpenAI-specific configuration. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class OpenAIConfig {
    /**
     * OpenAI API key (starts with "sk-"). Required when type is "openai". Can be set via
     * OPENAI_API_KEY environment variable.
     */
    private String apiKey;

    /**
     * OpenAI embedding model. Supported models:
     *
     * <ul>
     *   <li><b>text-embedding-3-large</b> (default): 3072 dimensions, highest quality
     *   <li><b>text-embedding-3-small</b>: 1536 dimensions, optimized for speed and cost
     *   <li><b>text-embedding-ada-002</b>: 1536 dimensions, legacy model
     * </ul>
     *
     * Defaults to "text-embedding-3-large".
     */
    private String model = "text-embedding-3-large";

    /**
     * OpenAI API endpoint. Defaults to "https://api.openai.com/v1/embeddings". For Azure OpenAI,
     * use:
     * "https://{resource-name}.openai.azure.com/openai/deployments/{deployment-id}/embeddings?api-version=2023-05-15"
     */
    private String endpoint = "https://api.openai.com/v1/embeddings";
  }

  /** Cohere-specific configuration. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CohereConfig {
    /**
     * Cohere API key. Required when type is "cohere". Can be set via COHERE_API_KEY environment
     * variable.
     */
    private String apiKey;

    /**
     * Cohere embedding model. Supported models:
     *
     * <ul>
     *   <li><b>embed-english-v3.0</b> (default): 1024 dimensions, English only
     *   <li><b>embed-multilingual-v3.0</b>: 1024 dimensions, 100+ languages
     *   <li><b>embed-english-light-v3.0</b>: 384 dimensions, faster and cheaper
     * </ul>
     *
     * Defaults to "embed-english-v3.0".
     */
    private String model = "embed-english-v3.0";

    /**
     * Cohere API endpoint. Defaults to "https://api.cohere.ai/v1/embed". For custom deployments,
     * specify the full embed endpoint URL.
     */
    private String endpoint = "https://api.cohere.ai/v1/embed";
  }
}
