package io.datahubproject.openapi.operations.maintenance.models;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Response containing current maintenance window status. */
@Value
@Builder
@Schema(description = "Current maintenance window status")
public class MaintenanceWindowStatus {

  @Schema(description = "Whether maintenance mode is currently active")
  boolean enabled;

  @Nullable
  @Schema(description = "Message displayed to users during maintenance")
  String message;

  @Nullable
  @Schema(description = "Severity level of the maintenance window")
  EnableMaintenanceRequest.MaintenanceSeverityDto severity;

  @Nullable
  @Schema(description = "URL for more details")
  String linkUrl;

  @Nullable
  @Schema(description = "Text for the link")
  String linkText;

  @Nullable
  @Schema(description = "Timestamp when maintenance mode was enabled (epoch millis)")
  Long enabledAt;

  @Nullable
  @Schema(description = "URN of user who enabled maintenance mode")
  String enabledBy;
}
