package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Data;

/** Filters for system metadata queries. */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Filters for system metadata queries")
public class SystemMetadataFilter {

  @Schema(description = "Only include aspects modified at or after this timestamp (epoch ms)")
  @Nullable
  private Long gePitEpochMs;

  @Schema(description = "Only include aspects modified at or before this timestamp (epoch ms)")
  @Nullable
  private Long lePitEpochMs;

  @Schema(
      description = "Only scan entities with ANY of these aspects in system metadata",
      example = "[\"assertionInfo\", \"assertionRunEvent\"]")
  @Nullable
  private List<String> aspectFilters;

  @Schema(description = "Include soft-deleted entities in scan", defaultValue = "false")
  private boolean includeSoftDeleted;

  /**
   * Convert to service-layer SystemMetadataFilter.
   *
   * @return service-layer filter
   */
  public com.linkedin.metadata.aspect.consistency.SystemMetadataFilter toServiceFilter() {
    return com.linkedin.metadata.aspect.consistency.SystemMetadataFilter.builder()
        .gePitEpochMs(gePitEpochMs)
        .lePitEpochMs(lePitEpochMs)
        .aspectFilters(aspectFilters)
        .includeSoftDeleted(includeSoftDeleted)
        .build();
  }
}
