package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for MCP-specific message size validation (Kafka ingestion/emission only). Will be
 * nested under metadataChangeProposal.validation.messageSize in application.yaml.
 *
 * <p>Note: For aspect size validation that applies to ALL writes (REST/GraphQL/MCP), see
 * AspectSizeValidationConfig under datahub.validation.aspectSize.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageSizeValidationConfig {
  /** Validates incoming MCP from Kafka consumer (measures: Kafka serialized byte size). */
  private CheckpointConfig incomingMcp = new CheckpointConfig(false, 4718592L);

  /** Validates outgoing MCL before Kafka producer (measures: Avro serialized byte size). */
  private CheckpointConfig outgoingMcl = new CheckpointConfig(false, 4718592L);

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class CheckpointConfig {
    private boolean enabled;
    private long maxSizeBytes;
  }
}
