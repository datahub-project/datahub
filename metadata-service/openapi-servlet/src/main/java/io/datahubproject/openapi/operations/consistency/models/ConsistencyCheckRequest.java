package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Data;

/** Request to run consistency checks on entities. */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Request to run consistency checks on entities")
public class ConsistencyCheckRequest {

  @Schema(
      description =
          "Entity type to check. " + "Use GET /consistency/checks to discover available types.",
      example = "assertion")
  @Nullable
  private String entityType;

  @Schema(
      description =
          "Specific check IDs to run. " + "Use GET /consistency/checks to discover available IDs.",
      example = "[\"assertion-entity-not-found\", \"assertion-monitor-missing\"]")
  @Nullable
  private List<String> checkIds;

  @Schema(description = "Maximum entities to scan per batch", example = "100", defaultValue = "100")
  @Min(1)
  @Max(1000)
  private int batchSize = 100;

  @Schema(
      description =
          "Scroll ID for pagination. Pass value from previous response to continue scanning.")
  @Nullable
  private String scrollId;

  @Schema(description = "Optional filters for system metadata queries")
  @Nullable
  private SystemMetadataFilter filter;

  @Schema(
      description =
          "Grace period in seconds. Entities modified within this window are excluded from checks "
              + "to avoid false positives from eventual consistency. Overrides server default "
              + "(300 seconds / 5 minutes). Set to 0 to disable.",
      example = "300")
  @Nullable
  private Long gracePeriodSeconds;
}
