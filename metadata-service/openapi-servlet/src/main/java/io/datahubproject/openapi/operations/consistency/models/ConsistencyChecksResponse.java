package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

/** Response containing all registered consistency checks. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Response containing all registered consistency checks")
public class ConsistencyChecksResponse {

  @Schema(description = "List of consistency checks")
  @Nonnull
  private List<ConsistencyCheckInfo> checks;
}
