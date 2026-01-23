package io.datahubproject.openapi.operations.maintenance;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.MaintenanceSeverity;
import com.linkedin.settings.global.MaintenanceWindowSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.operations.maintenance.models.EnableMaintenanceRequest;
import io.datahubproject.openapi.operations.maintenance.models.MaintenanceWindowStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for maintenance window operations API.
 *
 * <p>Allows administrators to enable/disable a maintenance mode banner that is displayed to all
 * authenticated users across the DataHub UI.
 */
@RestController
@RequestMapping("/openapi/operations/maintenance")
@Slf4j
@Tag(
    name = "Maintenance Window",
    description =
        "APIs for managing maintenance window announcements. "
            + "When enabled, a banner is displayed to all users. "
            + "Requires MANAGE_SYSTEM_OPERATIONS privilege to enable/disable.")
public class MaintenanceController {

  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;
  private final EntityClient entityClient;

  public MaintenanceController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      @Qualifier("systemEntityClient") EntityClient entityClient) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.entityClient = entityClient;
  }

  @Tag(name = "Maintenance Window")
  @GetMapping(path = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get maintenance window status",
      description =
          "Returns the current maintenance window configuration. "
              + "Available to any authenticated user.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Status retrieved successfully",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = MaintenanceWindowStatus.class))),
        @ApiResponse(responseCode = "401", description = "Not authenticated")
      })
  public ResponseEntity<?> getStatus(HttpServletRequest request) {
    // Only require authentication, no privilege check for reading status
    Authentication auth = AuthenticationContext.getAuthentication();
    if (auth == null) {
      return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
          .body(Map.of("error", "Authentication required"));
    }

    try {
      MaintenanceWindowSettings settings = getMaintenanceWindowSettings();
      return ResponseEntity.ok(mapToStatus(settings));
    } catch (Exception e) {
      log.error("Failed to get maintenance window status", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to retrieve maintenance window status"));
    }
  }

  @Tag(name = "Maintenance Window")
  @PostMapping(path = "/enable", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Enable maintenance mode",
      description =
          "Enables maintenance mode with the specified message, severity, and optional link. "
              + "Requires MANAGE_SYSTEM_OPERATIONS privilege.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Maintenance mode enabled successfully",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = MaintenanceWindowStatus.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "403", description = "Not authorized")
      })
  public ResponseEntity<?> enableMaintenance(
      HttpServletRequest request, @RequestBody EnableMaintenanceRequest req) {
    return withPrivilegedAccess(
        request,
        "enableMaintenance",
        () -> {
          if (req.getMessage() == null || req.getMessage().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Message is required"));
          }
          if (req.getSeverity() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Severity is required"));
          }

          try {
            Authentication auth = AuthenticationContext.getAuthentication();
            String actorUrn = auth.getActor().toUrnStr();

            // Fetch current global settings
            GlobalSettingsInfo globalSettings = getGlobalSettings();

            // Create or update maintenance window settings
            MaintenanceWindowSettings maintenanceSettings = new MaintenanceWindowSettings();
            maintenanceSettings.setEnabled(true);
            maintenanceSettings.setMessage(req.getMessage());
            maintenanceSettings.setSeverity(mapSeverity(req.getSeverity()));
            maintenanceSettings.setLinkUrl(req.getLinkUrl(), SetMode.IGNORE_NULL);
            maintenanceSettings.setLinkText(req.getLinkText(), SetMode.IGNORE_NULL);
            maintenanceSettings.setEnabledAt(System.currentTimeMillis());
            maintenanceSettings.setEnabledBy(actorUrn);

            globalSettings.setMaintenanceWindow(maintenanceSettings);

            // Write back synchronously
            persistGlobalSettings(globalSettings);

            log.info(
                "Maintenance mode enabled by {} with severity {}", actorUrn, req.getSeverity());
            return ResponseEntity.ok(mapToStatus(maintenanceSettings));

          } catch (Exception e) {
            log.error("Failed to enable maintenance mode", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to enable maintenance mode: " + e.getMessage()));
          }
        });
  }

  @Tag(name = "Maintenance Window")
  @PostMapping(path = "/disable", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Disable maintenance mode",
      description =
          "Disables maintenance mode and clears all settings. "
              + "Requires MANAGE_SYSTEM_OPERATIONS privilege.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Maintenance mode disabled successfully"),
        @ApiResponse(responseCode = "403", description = "Not authorized")
      })
  public ResponseEntity<?> disableMaintenance(HttpServletRequest request) {
    return withPrivilegedAccess(
        request,
        "disableMaintenance",
        () -> {
          try {
            Authentication auth = AuthenticationContext.getAuthentication();
            String actorUrn = auth.getActor().toUrnStr();

            // Fetch current global settings
            GlobalSettingsInfo globalSettings = getGlobalSettings();

            // Clear maintenance window settings
            MaintenanceWindowSettings maintenanceSettings = new MaintenanceWindowSettings();
            maintenanceSettings.setEnabled(false);
            // All other fields remain null/unset

            globalSettings.setMaintenanceWindow(maintenanceSettings);

            // Write back synchronously
            persistGlobalSettings(globalSettings);

            log.info("Maintenance mode disabled by {}", actorUrn);
            return ResponseEntity.ok(Map.of("message", "Maintenance mode disabled"));

          } catch (Exception e) {
            log.error("Failed to disable maintenance mode", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to disable maintenance mode: " + e.getMessage()));
          }
        });
  }

  // ==================== Helper Methods ====================

  /** Checks auth and privilege, then executes handler if authorized. */
  private ResponseEntity<?> withPrivilegedAccess(
      HttpServletRequest request, String operation, Supplier<ResponseEntity<?>> handler) {
    Authentication auth = AuthenticationContext.getAuthentication();
    if (auth == null) {
      return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
          .body(Map.of("error", "Authentication required"));
    }

    String actorUrn = auth.getActor().toUrnStr();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrn, request, operation, List.of()),
            authorizerChain,
            auth,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(Map.of("error", actorUrn + " is not authorized for maintenance operations"));
    }

    return handler.get();
  }

  /** Fetches the GlobalSettingsInfo aspect. */
  private GlobalSettingsInfo getGlobalSettings() throws Exception {
    EntityResponse entityResponse =
        entityClient.getV2(
            systemOperationContext,
            GLOBAL_SETTINGS_ENTITY_NAME,
            GLOBAL_SETTINGS_URN,
            ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME));

    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
      // Return empty settings if none exist
      return new GlobalSettingsInfo();
    }

    return new GlobalSettingsInfo(
        entityResponse.getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME).getValue().data());
  }

  /** Gets just the maintenance window settings, returning defaults if not set. */
  private MaintenanceWindowSettings getMaintenanceWindowSettings() throws Exception {
    GlobalSettingsInfo globalSettings = getGlobalSettings();
    if (globalSettings.hasMaintenanceWindow()) {
      return globalSettings.getMaintenanceWindow();
    }
    // Return default disabled state
    MaintenanceWindowSettings defaults = new MaintenanceWindowSettings();
    defaults.setEnabled(false);
    return defaults;
  }

  /** Persists the GlobalSettingsInfo aspect synchronously. */
  private void persistGlobalSettings(GlobalSettingsInfo settings) throws Exception {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(GLOBAL_SETTINGS_URN);
    proposal.setEntityType(GLOBAL_SETTINGS_ENTITY_NAME);
    proposal.setAspectName(GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(settings));
    proposal.setChangeType(ChangeType.UPSERT);

    // Synchronous ingestion (async=false)
    entityClient.ingestProposal(systemOperationContext, proposal, false);
  }

  /** Maps DTO severity to PDL enum. */
  private MaintenanceSeverity mapSeverity(EnableMaintenanceRequest.MaintenanceSeverityDto dto) {
    return switch (dto) {
      case INFO -> MaintenanceSeverity.INFO;
      case WARNING -> MaintenanceSeverity.WARNING;
      case CRITICAL -> MaintenanceSeverity.CRITICAL;
    };
  }

  /** Maps PDL settings to response DTO. */
  private MaintenanceWindowStatus mapToStatus(MaintenanceWindowSettings settings) {
    EnableMaintenanceRequest.MaintenanceSeverityDto severityDto = null;
    if (settings.hasSeverity()) {
      severityDto =
          switch (settings.getSeverity()) {
            case INFO -> EnableMaintenanceRequest.MaintenanceSeverityDto.INFO;
            case WARNING -> EnableMaintenanceRequest.MaintenanceSeverityDto.WARNING;
            case CRITICAL -> EnableMaintenanceRequest.MaintenanceSeverityDto.CRITICAL;
            default -> null;
          };
    }

    return MaintenanceWindowStatus.builder()
        .enabled(settings.isEnabled())
        .message(settings.hasMessage() ? settings.getMessage() : null)
        .severity(severityDto)
        .linkUrl(settings.hasLinkUrl() ? settings.getLinkUrl() : null)
        .linkText(settings.hasLinkText() ? settings.getLinkText() : null)
        .enabledAt(settings.hasEnabledAt() ? settings.getEnabledAt() : null)
        .enabledBy(settings.hasEnabledBy() ? settings.getEnabledBy() : null)
        .build();
  }
}
