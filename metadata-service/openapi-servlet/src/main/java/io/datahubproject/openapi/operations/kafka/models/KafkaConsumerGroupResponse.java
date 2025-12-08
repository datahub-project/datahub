package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka consumer group API endpoints. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka consumer group information")
public class KafkaConsumerGroupResponse {

  @Schema(description = "The consumer group ID", example = "generic-mce-consumer-job-client")
  private String groupId;

  @Schema(
      description = "The state of the consumer group",
      example = "Stable",
      allowableValues = {
        "Unknown",
        "PreparingRebalance",
        "CompletingRebalance",
        "Stable",
        "Dead",
        "Empty"
      })
  private String state;

  @Schema(description = "Whether the group uses simple consumer protocol")
  private Boolean isSimpleConsumerGroup;

  @Schema(description = "The coordinator broker information")
  private CoordinatorInfo coordinator;

  @Schema(description = "The partition assignor strategy", example = "range")
  private String partitionAssignor;

  @Schema(description = "List of members in the consumer group")
  private List<MemberInfo> members;

  @Schema(description = "Committed offsets per topic-partition")
  private Map<String, TopicOffsets> offsets;

  @Schema(description = "Total lag across all topic-partitions")
  private Long totalLag;

  /** Information about the group coordinator. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Consumer group coordinator information")
  public static class CoordinatorInfo {

    @Schema(description = "Coordinator broker ID", example = "0")
    private Integer id;

    @Schema(description = "Coordinator host", example = "kafka-0.kafka.svc.cluster.local")
    private String host;

    @Schema(description = "Coordinator port", example = "9092")
    private Integer port;
  }

  /** Information about a consumer group member. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Consumer group member information")
  public static class MemberInfo {

    @Schema(description = "The member ID assigned by the coordinator")
    private String memberId;

    @Schema(description = "The group instance ID for static membership, if configured")
    private String groupInstanceId;

    @Schema(description = "The client ID", example = "consumer-1")
    private String clientId;

    @Schema(description = "The client host", example = "/10.0.0.1")
    private String clientHost;

    @Schema(description = "List of topic-partitions assigned to this member")
    private List<TopicPartitionInfo> assignments;
  }

  /** Topic-partition assignment information. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Topic-partition assignment information")
  public static class TopicPartitionInfo {

    @Schema(description = "Topic name")
    private String topic;

    @Schema(description = "Partition ID")
    private Integer partition;
  }

  /** Offsets for a specific topic. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Offsets for a specific topic")
  public static class TopicOffsets {

    @Schema(description = "Map of partition ID to partition offset info")
    private Map<Integer, PartitionOffsetInfo> partitions;

    @Schema(description = "Total lag for this topic")
    private Long topicLag;
  }

  /** Offset information for a specific partition. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Partition offset information")
  public static class PartitionOffsetInfo {

    @Schema(description = "Current committed offset", example = "12345")
    private Long currentOffset;

    @Schema(description = "End offset (high watermark)", example = "12400")
    private Long endOffset;

    @Schema(description = "Lag (difference between end and current offset)", example = "55")
    private Long lag;

    @Schema(description = "Offset metadata, if any")
    private String metadata;
  }

  /** Simple response for consumer group list operations. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "List of consumer groups")
  public static class ConsumerGroupListResponse {

    @Schema(description = "List of consumer group summaries")
    private List<ConsumerGroupSummary> groups;

    @Schema(description = "Total number of consumer groups")
    private Integer count;
  }

  /** Summary information for a consumer group. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Consumer group summary")
  public static class ConsumerGroupSummary {

    @Schema(description = "The consumer group ID")
    private String groupId;

    @Schema(description = "The state of the consumer group")
    private String state;

    @Schema(description = "Whether this is a DataHub consumer group")
    private Boolean isDataHubGroup;

    @Schema(description = "Whether the group uses simple consumer protocol")
    private Boolean isSimpleConsumerGroup;
  }

  /** Response for offset reset operations. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Result of offset reset operation")
  public static class ResetOffsetsResponse {

    @Schema(description = "Whether this was a dry run")
    private Boolean dryRun;

    @Schema(description = "The consumer group ID")
    private String groupId;

    @Schema(description = "The topic that was reset")
    private String topic;

    @Schema(description = "Offset changes per partition")
    private List<PartitionResetInfo> partitionResets;

    @Schema(description = "Warning message, if any")
    private String warning;
  }

  /** Information about partition offset reset. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Partition offset reset information")
  public static class PartitionResetInfo {

    @Schema(description = "Partition ID")
    private Integer partition;

    @Schema(description = "Previous offset before reset")
    private Long previousOffset;

    @Schema(description = "New offset after reset")
    private Long newOffset;

    @Schema(description = "Change in offset (new - previous)")
    private Long offsetChange;
  }
}
