package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Result of resetting pgQueue consumer offsets")
public class ConsumerOffsetResetResponse {

  @Schema(description = "Active messaging transport", example = "pgqueue")
  private String transport;

  @Schema(description = "Number of partition offsets updated")
  private int partitionsUpdated;

  @Schema(description = "Per-partition reset details")
  private List<ConsumerOffsetResetDetailResponse> resets;
}
