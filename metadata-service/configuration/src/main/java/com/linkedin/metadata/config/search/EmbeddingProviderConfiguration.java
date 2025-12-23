package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for embedding providers used to generate query embeddings for semantic search. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmbeddingProviderConfiguration {

  /** Type of embedding provider. Currently supported: "aws-bedrock". Defaults to "aws-bedrock". */
  private String type = "aws-bedrock";

  /**
   * AWS region where Bedrock is available (e.g., "us-west-2", "us-east-1"). Required for
   * aws-bedrock provider.
   */
  private String awsRegion = "us-west-2";

  /**
   * Bedrock model ID for embeddings. Defaults to "cohere.embed-english-v3" (1024 dimensions). Other
   * options: - "cohere.embed-multilingual-v3" (1024 dimensions) - "amazon.titan-embed-text-v1"
   * (1536 dimensions) - "amazon.titan-embed-text-v2:0" (1024 dimensions default)
   */
  private String modelId = "cohere.embed-english-v3";

  /**
   * Maximum text length in characters before truncation. Cohere Embed v3 enforces a 2048-character
   * limit on the request body separate from the token context window. Defaults to 2048.
   */
  private int maxCharacterLength = 2048;
}
