package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

/** Result of fixing specific issues. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Result of fixing specific issues")
public class ConsistencyFixIssuesResult {

  /** Creates a ConsistencyFixIssuesResult from a service-layer FixResult. */
  public static ConsistencyFixIssuesResult from(@Nonnull ConsistencyFixResult result) {
    return ConsistencyFixIssuesResult.builder()
        .dryRun(result.isDryRun())
        .totalProcessed(result.getTotalProcessed())
        .entitiesFixed(result.getEntitiesFixed())
        .entitiesFailed(result.getEntitiesFailed())
        .fixDetails(FixDetail.from(result.getFixDetails()))
        .build();
  }

  @Schema(description = "Whether this was a dry-run (no actual changes made)")
  private boolean dryRun;

  @Schema(description = "Total number of issues processed")
  private int totalProcessed;

  @Schema(description = "Number of entities successfully fixed (or would be in dry-run)")
  private int entitiesFixed;

  @Schema(description = "Number of entities that failed to fix")
  private int entitiesFailed;

  @Schema(description = "Details of each fix operation")
  @Nonnull
  private List<FixDetail> fixDetails;
}
