package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

/** Information about a registered consistency check. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Information about a registered consistency check")
public class ConsistencyCheckInfo {

  /** Creates a ConsistencyCheckInfo from a service-layer ConsistencyCheck. */
  public static ConsistencyCheckInfo from(@Nonnull ConsistencyCheck check) {
    return ConsistencyCheckInfo.builder()
        .id(check.getId())
        .name(check.getName())
        .description(check.getDescription())
        .entityType(check.getEntityType())
        .onDemandOnly(check.isOnDemandOnly())
        .requiredAspects(check.getRequiredAspects().orElse(null))
        .requiresAllAspects(check.getRequiredAspects().isEmpty())
        .targetAspects(check.getTargetAspects())
        .build();
  }

  /** Creates a list of ConsistencyCheckInfo from ConsistencyChecks. */
  public static List<ConsistencyCheckInfo> from(@Nonnull List<ConsistencyCheck> checks) {
    return checks.stream().map(ConsistencyCheckInfo::from).collect(Collectors.toList());
  }

  @Schema(description = "Unique check identifier", example = "assertion-entity-not-found")
  @Nonnull
  private String id;

  @Schema(description = "Human-readable check name", example = "Assertion Entity Not Found")
  @Nonnull
  private String name;

  @Schema(description = "Detailed description of what this check validates")
  @Nonnull
  private String description;

  @Schema(description = "Entity type this check applies to", example = "assertion")
  @Nonnull
  private String entityType;

  @Schema(description = "Whether this check only runs when explicitly requested")
  private boolean onDemandOnly;

  @Schema(
      description =
          "Aspects that must be fetched for this check to run. Null if requiresAllAspects is true.")
  private Set<String> requiredAspects;

  @Schema(description = "Whether this check requires all aspects to be fetched")
  private boolean requiresAllAspects;

  @Schema(description = "Aspects this check targets for filtering in system metadata")
  private Set<String> targetAspects;
}
