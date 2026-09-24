package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Register or unregister a consumer group for a topic")
public class ConsumerRegistrationRequest {

  @Schema(description = "Consumer group identifier", example = "generic-mce-consumer-job-client")
  private String consumerGroup;

  @Schema(description = "Logical pgQueue topic name", example = "MetadataChangeProposal_v1")
  private String topicName;
}
