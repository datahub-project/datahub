package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "One partition whose offset was reset")
public class ConsumerOffsetResetDetailResponse {

  private String consumerGroup;
  private String topicName;
  private int partitionId;
  private long previousOffset;
  private long newOffset;
  private long maxSeq;
}
