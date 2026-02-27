package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/** Result of a check-and-fix operation. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Result of a check-and-fix operation")
public class ConsistencyFixResult {

  /**
   * Creates a ConsistencyFixResult from check and fix results.
   *
   * @param checkResult the check result
   * @param fixResult the fix result
   * @return combined result
   */
  public static ConsistencyFixResult from(
      @Nonnull CheckResult checkResult,
      @Nonnull com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult fixResult) {
    return io.datahubproject.openapi.operations.consistency.models.ConsistencyFixResult.builder()
        .entitiesScanned(checkResult.getEntitiesScanned())
        .issuesFound(checkResult.getIssuesFound())
        .scrollId(checkResult.getScrollId())
        .dryRun(fixResult.isDryRun())
        .totalProcessed(fixResult.getTotalProcessed())
        .entitiesFixed(fixResult.getEntitiesFixed())
        .entitiesFailed(fixResult.getEntitiesFailed())
        .fixDetails(FixDetail.from(fixResult.getFixDetails()))
        .build();
  }

  @Schema(description = "Number of entities scanned in this batch")
  private int entitiesScanned;

  @Schema(description = "Number of issues found")
  private int issuesFound;

  @Schema(
      description =
          "Scroll ID for pagination. Null if no more results. Pass to next request to continue.")
  @Nullable
  private String scrollId;

  @Schema(description = "Whether this was a dry-run (no actual changes made)")
  private boolean dryRun;

  @Schema(description = "Total number of issues processed for fixes")
  private int totalProcessed;

  @Schema(description = "Number of entities successfully fixed (or would be in dry-run)")
  private int entitiesFixed;

  @Schema(description = "Number of entities that failed to fix")
  private int entitiesFailed;

  @Schema(description = "Details of each fix operation")
  @Nonnull
  private List<FixDetail> fixDetails;
}
