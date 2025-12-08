package io.datahubproject.openapi.operations.kafka.models;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Response model for Kafka cluster info API endpoint. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Kafka cluster metadata information")
public class KafkaClusterResponse {

  @Schema(description = "The cluster ID", example = "lkc-abc123")
  private String clusterId;

  @Schema(description = "The controller broker information")
  private BrokerInfo controller;

  @Schema(description = "List of all brokers in the cluster")
  private List<BrokerInfo> brokers;

  @Schema(description = "Total number of brokers in the cluster")
  private Integer brokerCount;

  /** Information about a Kafka broker. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @Schema(description = "Information about a Kafka broker")
  public static class BrokerInfo {

    @Schema(description = "The broker ID", example = "0")
    private Integer id;

    @Schema(description = "The broker host", example = "kafka-0.kafka.default.svc.cluster.local")
    private String host;

    @Schema(description = "The broker port", example = "9092")
    private Integer port;

    @Schema(description = "The rack of the broker, if configured", example = "us-east-1a")
    private String rack;

    @Schema(description = "Whether this broker is the controller")
    private Boolean isController;
  }
}
