package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Reset pgQueue consumer offsets to the current message log end")
public class ConsumerOffsetResetRequest {

  @Schema(
      description = "Consumer group to reset (omit to match all groups)",
      example = "generic-mae-consumer-job-client")
  private String consumerGroup;

  @Schema(
      description = "Logical topic name (omit to match all topics)",
      example = "MetadataChangeLog_Versioned_v1")
  private String topicName;

  @Schema(description = "Partition id (omit to reset all partitions for the selection)")
  private Integer partitionId;

  @Schema(
      description =
          "When true, only reset STUCK_AHEAD partitions (committed offset ahead of the log). "
              + "When false, set every selected partition to the log end.",
      defaultValue = "true")
  private boolean onlyStuckAhead = true;
}
