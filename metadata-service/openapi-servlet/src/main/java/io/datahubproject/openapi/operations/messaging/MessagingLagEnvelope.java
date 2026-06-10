package io.datahubproject.openapi.operations.messaging;

import io.datahubproject.openapi.operations.kafka.KafkaOffsetResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Transport-agnostic wrapper for consumer lag / offset payloads (same shape as legacy Kafka API).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Active messaging transport and consumer-group offset/lag data")
public class MessagingLagEnvelope {

  @Schema(description = "kafka or pgqueue", example = "kafka")
  private String transport;

  @Schema(description = "Consumer group id to topic offset map (legacy KafkaOffsetResponse shape)")
  private KafkaOffsetResponse consumerGroups;
}
