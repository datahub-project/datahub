package io.datahubproject.openapi.operations.maintenance.models;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Request body for enabling maintenance mode. */
@Data
@NoArgsConstructor
@Schema(description = "Request to enable maintenance mode")
public class EnableMaintenanceRequest {

  @Nonnull
  @Schema(
      description = "Message to display to users during maintenance",
      example = "Scheduled maintenance in progress. Some features may be unavailable.",
      required = true)
  private String message;

  @Nonnull
  @Schema(
      description =
          "Severity level affecting banner color. Possible values: INFO (blue), WARNING (yellow), CRITICAL (red)",
      example = "WARNING",
      required = true)
  private MaintenanceSeverityDto severity;

  @Nullable
  @Schema(
      description = "Optional URL for more details (e.g., status page)",
      example = "https://status.example.com")
  private String linkUrl;

  @Nullable
  @Schema(
      description = "Optional text for the link (defaults to 'Learn more' if linkUrl provided)",
      example = "View status page")
  private String linkText;

  /** Severity levels for maintenance window announcements. */
  public enum MaintenanceSeverityDto {
    INFO,
    WARNING,
    CRITICAL
  }
}
