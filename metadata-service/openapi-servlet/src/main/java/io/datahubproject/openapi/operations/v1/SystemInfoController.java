package io.datahubproject.openapi.operations.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.system_info.SystemInfoResponse;
import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/openapi/v1/system-info")
@Tag(
    name = "System Information",
    description = "APIs for retrieving system configuration and component information")
public class SystemInfoController {

  private final SystemInfoService systemInfoService;
  private final ObjectMapper objectMapper;

  @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get complete system information",
      description =
          "Retrieves comprehensive system information including Spring components and system properties with metadata")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system information"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSystemInfo() {
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
      description = "Retrieves information about Spring beans and components")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved Spring components information"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSpringComponentsInfo() {
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
      summary = "Get system properties with metadata",
      description =
          "Retrieves system properties with detailed metadata including sources and filtering information")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved system properties"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getSystemPropertiesInfo() {
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
      summary = "Get configuration properties as simple map",
      description =
          "Retrieves system configuration properties as a simple key-value map (for backward compatibility)")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved configuration properties"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  public ResponseEntity<String> getPropertiesAsMap() {
    try {
      log.debug("Request received for configuration properties as map");
      Map<String, Object> response = systemInfoService.getPropertiesAsMap();

      // Return pretty-printed JSON for better readability in debugging/admin scenarios.
      // This follows the same pattern used by /config endpoints in the codebase.
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
    } catch (Exception e) {
      log.error("Error retrieving configuration properties", e);
      return ResponseEntity.internalServerError().build();
    }
  }
}
