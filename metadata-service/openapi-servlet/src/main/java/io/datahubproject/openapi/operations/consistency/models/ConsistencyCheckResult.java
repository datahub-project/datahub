package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/** Result of a consistency check operation. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Result of a consistency check operation")
public class ConsistencyCheckResult {

  /** Creates a ConsistencyCheckResult from a service-layer CheckResult. */
  public static ConsistencyCheckResult from(@Nonnull CheckResult result) {
    return from(result, null);
  }

  /** Creates a ConsistencyCheckResult with an optional total estimate from countMatching. */
  public static ConsistencyCheckResult from(
      @Nonnull CheckResult result, @Nullable Long totalEstimate) {
    return ConsistencyCheckResult.builder()
        .entitiesScanned(result.getEntitiesScanned())
        .issuesFound(result.getIssuesFound())
        .issues(ConsistencyIssue.from(result.getIssues()))
        .scrollId(result.getScrollId())
        .totalEstimate(totalEstimate)
        .build();
  }

  @Schema(description = "Number of entities scanned in this batch")
  private int entitiesScanned;

  @Schema(description = "Number of issues found")
  private int issuesFound;

  @Schema(description = "List of consistency issues found")
  @Nonnull
  private List<ConsistencyIssue> issues;

  @Schema(
      description =
          "Scroll ID for pagination. Null if no more results. Pass to next request to continue.")
  @Nullable
  private String scrollId;

  @Schema(
      description =
          "Estimated matching system-metadata documents for this request filter. "
              + "Populated on the first request page (when no scroll ID is supplied) when a count "
              + "is available. With keyAspectOnly, approximates unique entities.")
  @Nullable
  private Long totalEstimate;
}
