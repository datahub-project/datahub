package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka topic API endpoints. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka topic information")
public class KafkaTopicResponse {

  @Schema(description = "The topic name", example = "MetadataChangeProposal_v1")
  private String name;

  @Schema(description = "Number of partitions in the topic", example = "3")
  private Integer partitionCount;

  @Schema(description = "Replication factor for the topic", example = "3")
  private Integer replicationFactor;

  @Schema(description = "Whether this is an internal Kafka topic")
  private Boolean internal;

  @Schema(description = "DataHub topic alias if this is a known DataHub topic", example = "mcp")
  private String dataHubAlias;

  @Schema(description = "Partition details including leader, replicas, and ISR")
  private List<PartitionInfo> partitions;

  @Schema(description = "Topic configuration key-value pairs")
  private Map<String, String> configs;

  /** Information about a topic partition. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a topic partition")
  public static class PartitionInfo {

    @Schema(description = "The partition ID", example = "0")
    private Integer partition;

    @Schema(description = "The leader broker ID for this partition", example = "1")
    private Integer leader;

    @Schema(description = "List of replica broker IDs")
    private List<Integer> replicas;

    @Schema(description = "List of in-sync replica (ISR) broker IDs")
    private List<Integer> isr;

    @Schema(description = "Beginning offset for this partition")
    private Long beginningOffset;

    @Schema(description = "End offset for this partition")
    private Long endOffset;
  }

  /** Simple response for topic list operations. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "List of Kafka topics")
  public static class TopicListResponse {

    @Schema(description = "List of topic names")
    private List<String> topics;

    @Schema(description = "Total number of topics")
    private Integer count;

    @Schema(description = "Whether internal topics are included")
    private Boolean includeInternal;
  }
}
