package io.datahubproject.openapi.operations.v1;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.system_info.SystemInfoResponse;
import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for system information endpoints.
 *
 * <p>Provides system information through separate endpoints:
 *
 * <ul>
 *   <li>/system-info - Spring component status (GMS, MAE Consumer, MCE Consumer)
 *   <li>/system-info/spring-components - Just component information
 *   <li>/system-info/properties - Detailed system properties with metadata
 *   <li>/system-info/properties/simple - Simple key-value property map
 * </ul>
 *
 * <p>This separation allows clients to fetch only the information they need, avoiding response
 * complexity and data duplication.
 *
 * <p>All endpoints return pretty-printed JSON for better readability in debugging/admin scenarios.
 * This follows the same pattern used by /config endpoints in the codebase.
 *
 * <p><strong>Security Note:</strong> These endpoints expose system configuration data and are
 * restricted to administrators with MANAGE_SYSTEM_OPERATIONS_PRIVILEGE. Sensitive properties
 * (passwords, secrets, keys) are automatically redacted as ***REDACTED***.
 */
@Slf4j
@RestController
@RequestMapping("/openapi/v1/system-info")
@Tag(
    name = "System Information",
    description = "APIs for retrieving system configuration and component information")
public class SystemInfoController {

  private final SystemInfoService systemInfoService;
  private final ObjectMapper objectMapper;
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;

  public SystemInfoController(
      SystemInfoService systemInfoService,
      ObjectMapper objectMapper,
      AuthorizerChain authorizerChain,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.systemInfoService = systemInfoService;
    this.objectMapper = objectMapper;
    this.authorizerChain = authorizerChain;
    this.systemOperationContext = systemOperationContext;
  }

  /**
   * Checks if the current user is authorized to access system information endpoints.
   *
   * @param request The HTTP request
   * @return ResponseEntity with error if unauthorized, null if authorized
   */
  private ResponseEntity<String> checkAuthorization(HttpServletRequest request) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, "systemInfo", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body("{\"error\": \"" + actorUrnStr + " is not authorized for system operations.\"}");
    }
    return null; // Authorization successful
  }

  @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get system information",
      description =
          "Retrieves Spring component information including GMS, MAE Consumer, and MCE Consumer status. "
              + "For detailed system properties, use the /properties endpoint. "
              + "Returns pretty-printed JSON for better readability. "
              + "Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system information"),
        @ApiResponse(
            responseCode = "403",
            description = "Access forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSystemInfo(HttpServletRequest request) {
    ResponseEntity<String> authCheck = checkAuthorization(request);
    if (authCheck != null) return authCheck;
    try {
      log.debug("Request received for complete system information");
      SystemInfoResponse response = systemInfoService.getSystemInfo();

      // Return pretty-printed JSON for better readability in debugging/admin scenarios.
      // This follows the same pattern used by /config endpoints in the codebase.
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (Exception e) {
      log.error("Error retrieving system information", e);
      return ResponseEntity.internalServerError().build();
    }
  }

  @GetMapping(path = "/spring-components", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get Spring components information",
      description =
          "Retrieves information about Spring components including GMS, MAE Consumer, and MCE Consumer status. "
              + "Returns pretty-printed JSON for better readability. "
              + "Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved Spring components information"),
        @ApiResponse(
            responseCode = "403",
            description = "Access forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSpringComponentsInfo(HttpServletRequest request) {
    ResponseEntity<String> authCheck = checkAuthorization(request);
    if (authCheck != null) return authCheck;
    try {
      log.debug("Request received for Spring components information");
      SpringComponentsInfo response = systemInfoService.getSpringComponentsInfo();

      // Return pretty-printed JSON for better readability in debugging/admin scenarios.
      // This follows the same pattern used by /config endpoints in the codebase.
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (Exception e) {
      log.error("Error retrieving Spring components information", e);
      return ResponseEntity.internalServerError().build();
    }
  }

  @GetMapping(path = "/properties", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get detailed system properties",
      description =
          "Retrieves detailed system properties information with metadata including property sources and filtering. "
              + "Returns pretty-printed JSON for better readability. Sensitive properties are automatically redacted. "
              + "Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system properties information"),
        @ApiResponse(
            responseCode = "403",
            description = "Access forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSystemPropertiesInfo(HttpServletRequest request) {
    ResponseEntity<String> authCheck = checkAuthorization(request);
    if (authCheck != null) return authCheck;
    try {
      log.debug("Request received for system properties information");
      SystemPropertiesInfo response = systemInfoService.getSystemPropertiesInfo();

      // Return pretty-printed JSON for better readability in debugging/admin scenarios.
      // This follows the same pattern used by /config endpoints in the codebase.
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (Exception e) {
      log.error("Error retrieving system properties information", e);
      return ResponseEntity.internalServerError().build();
    }
  }

  @GetMapping(path = "/properties/simple", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get simple system properties map",
      description =
          "Retrieves system properties as a simple key-value map for backward compatibility. "
              + "Returns pretty-printed JSON for better readability. Sensitive properties are automatically redacted. "
              + "Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system properties map"),
        @ApiResponse(
            responseCode = "403",
            description = "Access forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getPropertiesAsMap(HttpServletRequest request) {
    ResponseEntity<String> authCheck = checkAuthorization(request);
    if (authCheck != null) return authCheck;
    try {
      log.debug("Request received for simple system properties map");
      Map<String, Object> response = systemInfoService.getPropertiesAsMap();

      // Return pretty-printed JSON for better readability in debugging/admin scenarios.
      // This follows the same pattern used by /config endpoints in the codebase.
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (Exception e) {
      log.error("Error retrieving system properties map", e);
      return ResponseEntity.internalServerError().build();
    }
  }
}
