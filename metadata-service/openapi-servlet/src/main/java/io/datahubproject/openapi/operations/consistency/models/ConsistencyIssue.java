package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** A consistency issue found during validation. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "A consistency issue found during validation")
public class ConsistencyIssue {

  /** Creates a ConsistencyIssue from a service-layer Issue. */
  public static ConsistencyIssue from(
      @Nonnull com.linkedin.metadata.aspect.consistency.ConsistencyIssue issue) {
    return io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.builder()
        .entityUrn(issue.getEntityUrn().toString())
        .entityType(issue.getEntityType())
        .checkId(issue.getCheckId())
        .fixType(issue.getFixType())
        .description(issue.getDescription())
        .relatedUrns(
            issue.getRelatedUrns() != null
                ? issue.getRelatedUrns().stream().map(Urn::toString).collect(Collectors.toList())
                : null)
        .details(issue.getDetails())
        .build();
  }

  /** Creates a list of ConsistencyIssues from service-layer Issues. */
  public static List<ConsistencyIssue> from(
      @Nonnull List<com.linkedin.metadata.aspect.consistency.ConsistencyIssue> issues) {
    return issues.stream()
        .map(io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue::from)
        .collect(Collectors.toList());
  }

  @Schema(description = "URN of the entity with the issue", example = "urn:li:assertion:abc123")
  @Nonnull
  private String entityUrn;

  @Schema(
      description = "Entity type. If not provided, derived from entityUrn.",
      example = "assertion")
  @Nullable
  private String entityType;

  @Schema(
      description = "ID of the check that found this issue. Optional for fix-issues endpoint.",
      example = "assertion-entity-not-found")
  @Nullable
  private String checkId;

  @Schema(
      description =
          "Type of fix to apply. If not provided with checkId, the check will be run to determine the fix type.")
  @Nullable
  private ConsistencyFixType fixType;

  @Schema(
      description = "Human-readable description of the issue. Optional for fix-issues endpoint.")
  @Nullable
  private String description;

  @Schema(description = "Related entity URNs (e.g., missing referenced entities)")
  @Nullable
  private List<String> relatedUrns;

  @Schema(description = "Additional details about the issue")
  @Nullable
  private String details;
}
