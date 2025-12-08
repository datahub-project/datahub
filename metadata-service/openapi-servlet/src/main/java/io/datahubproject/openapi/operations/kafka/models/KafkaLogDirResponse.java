package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka log directories API endpoint. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka log directory information for brokers")
public class KafkaLogDirResponse {

  @Schema(description = "List of broker log directory information")
  private List<BrokerLogDirInfo> brokers;

  /** Log directory information for a specific broker. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Log directory information for a broker")
  public static class BrokerLogDirInfo {

    @Schema(description = "The broker ID", example = "0")
    private Integer brokerId;

    @Schema(description = "List of log directories on this broker")
    private List<LogDirInfo> logDirs;

    @Schema(description = "Total size across all log directories in bytes")
    private Long totalSizeBytes;

    @Schema(description = "Error message if unable to retrieve log dir info for this broker")
    private String error;
  }

  /** Information about a single log directory. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a single log directory")
  public static class LogDirInfo {

    @Schema(description = "The log directory path", example = "/var/kafka-logs")
    private String path;

    @Schema(description = "Error for this log directory, if any")
    private String error;

    @Schema(description = "Total size of all replicas in this log directory in bytes")
    private Long sizeBytes;

    @Schema(description = "Map of topic-partition to replica info")
    private Map<String, ReplicaInfo> replicas;
  }

  /** Information about a replica in a log directory. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a replica in a log directory")
  public static class ReplicaInfo {

    @Schema(description = "Topic name")
    private String topic;

    @Schema(description = "Partition ID")
    private Integer partition;

    @Schema(description = "Size of the replica in bytes")
    private Long sizeBytes;

    @Schema(description = "Offset lag of the replica")
    private Long offsetLag;

    @Schema(description = "Whether this replica is a future replica (being moved)")
    private Boolean isFuture;
  }
}
