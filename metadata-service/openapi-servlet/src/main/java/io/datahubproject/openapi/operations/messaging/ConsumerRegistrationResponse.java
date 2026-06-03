package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "A registered consumer group for a topic")
public class ConsumerRegistrationResponse {

  @Schema(description = "Consumer group identifier")
  private String consumerGroup;

  @Schema(description = "Logical pgQueue topic name")
  private String topicName;

  @Schema(description = "When the consumer was first registered (epoch millis)")
  private Long registeredAt;

  @Schema(description = "Last heartbeat timestamp in epoch millis (updated on every poll cycle)")
  private Long lastHeartbeatAt;
}
