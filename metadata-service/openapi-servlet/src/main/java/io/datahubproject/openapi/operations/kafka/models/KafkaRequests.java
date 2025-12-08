package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Request models for Kafka admin API endpoints. */
public final class KafkaRequests {

  private KafkaRequests() {}

  /** Request model for creating a topic. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Request to create a Kafka topic")
  public static class CreateTopicRequest {

    @Schema(description = "Number of partitions for the topic", example = "3", defaultValue = "1")
    private Integer numPartitions;

    @Schema(
        description = "Replication factor for the topic",
        example = "3",
        defaultValue = "Uses cluster default")
    private Short replicationFactor;

    @Schema(description = "Topic configuration key-value pairs")
    private Map<String, String> configs;
  }

  /** Request model for altering topic configs. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Request to alter topic configuration")
  public static class AlterConfigRequest {

    @Schema(
        description = "Configuration changes to apply. Set value to null to delete a config.",
        example = "{\"retention.ms\": \"604800000\", \"cleanup.policy\": \"delete\"}")
    private Map<String, String> configs;
  }

  /**
   * Request model for resetting consumer group offsets.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Reset to beginning: {"topic": "mcp", "strategy": "earliest", "dryRun": true}
   *   <li>Reset to end: {"topic": "mcl-versioned", "strategy": "latest"}
   *   <li>Reset to specific offset: {"topic": "mcp", "strategy": "to-offset", "value": "1000"}
   *   <li>Move forward 100: {"topic": "mcp", "strategy": "shift-by", "value": "100"}
   *   <li>Move backward 50: {"topic": "mcp", "strategy": "shift-by", "value": "-50"}
   *   <li>Reset specific partitions: {"topic": "mcp", "strategy": "earliest", "partitions": [0, 1]}
   * </ul>
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(
      description =
          "Request to reset consumer group offsets for a specific DataHub topic. "
              + "IMPORTANT: Consumer group should be stopped before resetting offsets.")
  public static class ResetOffsetsRequest {

    @Schema(
        description =
            "Topic alias to reset offsets for. Must be a DataHub topic alias:\n"
                + "- **mcp**: MetadataChangeProposal topic\n"
                + "- **mcl-versioned**: MetadataChangeLog versioned topic\n"
                + "- **mcl-timeseries**: MetadataChangeLog timeseries topic\n"
                + "- **platform-event**: Platform event topic\n"
                + "- **upgrade-history**: Upgrade history topic\n"
                + "- **mcp-failed**: Failed MCP topic",
        example = "mcp",
        requiredMode = Schema.RequiredMode.REQUIRED,
        allowableValues = {
          "mcp",
          "mcp-failed",
          "mcl-versioned",
          "mcl-timeseries",
          "platform-event",
          "upgrade-history"
        })
    private String topic;

    @Schema(
        description =
            "Reset strategy. Options:\n"
                + "- **earliest**: Reset to the beginning of the topic (first available offset)\n"
                + "- **latest**: Reset to the end of the topic (skip all existing messages)\n"
                + "- **to-offset**: Reset to a specific offset number (requires 'value' field)\n"
                + "- **shift-by**: Move offset forward or backward by N messages (requires 'value' field, can be negative)",
        example = "earliest",
        requiredMode = Schema.RequiredMode.REQUIRED,
        allowableValues = {"earliest", "latest", "to-offset", "shift-by"})
    private String strategy;

    @Schema(
        description =
            "Target value - meaning depends on strategy:\n"
                + "- **to-offset**: The exact offset number to reset to (e.g., \"1000\")\n"
                + "- **shift-by**: Number of messages to shift (positive = forward, negative = backward, e.g., \"-100\")\n"
                + "- Not used for 'earliest' or 'latest' strategies",
        example = "1000")
    private String value;

    @Schema(
        description =
            "Specific partition numbers to reset. If not provided, all partitions of the topic "
                + "assigned to the consumer group are reset.",
        example = "[0, 1, 2]")
    private List<Integer> partitions;

    @Schema(
        description =
            "If true, shows what offsets would be changed without actually committing the reset. "
                + "Recommended to run with dryRun=true first to preview changes.",
        example = "true",
        defaultValue = "false")
    private Boolean dryRun;
  }

  /** Request model for replaying failed MCPs. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Request to replay failed MCPs from the mcp-failed topic")
  public static class ReplayRequest {

    @Schema(description = "Partition to replay from", example = "0")
    private Integer partition;

    @Schema(
        description = "Starting offset to replay from (inclusive)",
        example = "1000",
        requiredMode = Schema.RequiredMode.REQUIRED)
    private Long startOffset;

    @Schema(
        description =
            "Ending offset to replay to (inclusive). If not provided, replays only startOffset.",
        example = "1010")
    private Long endOffset;

    @Schema(
        description = "Maximum number of messages to replay. Applied after offset range filtering.",
        example = "100",
        defaultValue = "100")
    private Integer count;

    @Schema(
        description = "If true, only shows what would be replayed without actually publishing",
        defaultValue = "false")
    private Boolean dryRun;
  }
}
