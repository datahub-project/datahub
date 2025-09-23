package io.datahubproject.openapi.operations.kafka;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka consumer offsets API endpoint. */
@Schema(description = "Kafka consumer group offset information with lag metrics")
public class KafkaOffsetResponse extends LinkedHashMap<String, Object> {

  /** Class representing information for a specific topic. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information for a specific Kafka topic")
  public static class TopicOffsetInfo {

    @Schema(description = "Map of partition ID to partition offset information")
    private Map<String, PartitionInfo> partitions;

    @Schema(description = "Aggregate metrics for this topic")
    private LagMetrics metrics;
  }

  /** Class representing information for a specific partition. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information for a specific Kafka partition")
  public static class PartitionInfo {

    @Schema(description = "Current consumer offset")
    private Long offset;

    @Schema(description = "Additional metadata for this offset, if available")
    private String metadata;

    @Schema(description = "Consumer lag (difference between end offset and consumer offset)")
    private Long lag;
  }

  /** Class representing aggregate lag metrics for a topic. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Aggregated lag metrics across all partitions of a topic")
  public static class LagMetrics {

    @Schema(description = "Maximum lag across all partitions")
    private Long maxLag;

    @Schema(description = "Median lag across all partitions")
    private Long medianLag;

    @Schema(description = "Total lag across all partitions")
    private Long totalLag;

    @Schema(description = "Average lag across all partitions (rounded)")
    private Long avgLag;
  }
}
