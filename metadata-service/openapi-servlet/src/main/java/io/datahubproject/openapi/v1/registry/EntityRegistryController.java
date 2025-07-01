package io.datahubproject.openapi.v1.registry;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.v1.models.registry.AspectSpecDto;
import io.datahubproject.openapi.v1.models.registry.EntitySpecDto;
import io.datahubproject.openapi.v1.models.registry.EntitySpecResponse;
import io.datahubproject.openapi.v1.models.registry.EventSpecDto;
import io.datahubproject.openapi.v1.models.registry.PaginatedResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/registry/models")
@Slf4j
@Tag(name = "Entity Registry API", description = "An API to expose the Entity Registry")
@AllArgsConstructor
@NoArgsConstructor
public class EntityRegistryController {
  @Autowired private AuthorizerChain authorizerChain;
  @Autowired private OperationContext systemOperationContext;

  @GetMapping(path = "/entity/specifications", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description = "Retrieves all entity specs. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned entity specs",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<PaginatedResponse<EntitySpecDto>> getEntitySpecs(
      HttpServletRequest request,
      @Parameter(description = "Start index for pagination", example = "0")
          @RequestParam(name = "start", required = false, defaultValue = "0")
          Integer start,
      @Parameter(name = "count", description = "Number of items to return", example = "5")
          @RequestParam(name = "count", required = false, defaultValue = "5")
          Integer count) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getEntitySpecs", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get entity", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    Map<String, EntitySpec> entitySpecs =
        systemOperationContext.getEntityRegistry().getEntitySpecs();

    PaginatedResponse<EntitySpecDto> response =
        PaginatedResponse.fromMap(
            entitySpecs,
            start,
            count,
            entry -> {
              EntitySpecDto dto = EntitySpecDto.fromEntitySpec(entry.getValue());
              // Ensure the name is set from the map key
              if (dto.getName() == null) {
                dto.setName(entry.getKey());
              }
              return dto;
            });

    return ResponseEntity.ok(response);
  }

  @GetMapping(
      path = "/entity/specifications/{entityName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves entity spec for entity. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned entity spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<EntitySpecResponse> getEntitySpec(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getEntitySpec", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get entity", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    try {

      EntitySpec entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(entityName);

      return ResponseEntity.ok(EntitySpecResponse.fromEntitySpec(entitySpec));

    } catch (IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("Failed to find entity with name")) {
        return ResponseEntity.notFound().build();
      }
      throw e;
    }
  }

  @GetMapping(
      path = "/entity/specifications/{entityName}/aspects",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves aspect specs for the entity. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned aspect specs",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<Map<String, AspectSpecDto>> getEntityAspecSpecs(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getEntityAspecSpecs", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get entity model", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    try {

      Map<String, AspectSpec> aspectSpecs =
          systemOperationContext.getEntityRegistry().getEntitySpec(entityName).getAspectSpecMap();
      Map<String, AspectSpecDto> aspectSpecDtos =
          aspectSpecs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, entry -> AspectSpecDto.fromAspectSpec(entry.getValue())));
      return ResponseEntity.ok(aspectSpecDtos);

    } catch (IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("Failed to find entity with name")) {
        return ResponseEntity.notFound().build();
      }
      throw e;
    }
  }

  @GetMapping(path = "/aspect/specifications", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description = "Retrieves all entity specs. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned entity specs",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<PaginatedResponse<AspectSpecDto>> getAspectSpecs(
      HttpServletRequest request,
      @Parameter(description = "Start index for pagination", example = "0")
          @RequestParam(name = "start", required = false, defaultValue = "0")
          Integer start,
      @Parameter(name = "count", description = "Number of items to return", example = "20")
          @RequestParam(name = "count", required = false, defaultValue = "20")
          Integer count) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getAspectSpecs", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get aspect models", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    Map<String, AspectSpec> aspectSpecs =
        systemOperationContext.getEntityRegistry().getAspectSpecs();

    PaginatedResponse<AspectSpecDto> response =
        PaginatedResponse.fromMap(
            aspectSpecs, start, count, entry -> AspectSpecDto.fromAspectSpec(entry.getValue()));

    return ResponseEntity.ok(response);
  }

  @GetMapping(
      path = "/aspect/specifications/{aspectName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves aspect spec for the specified aspect. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned aspect spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<AspectSpecDto> getAspectSpec(
      HttpServletRequest request, @PathVariable("aspectName") String aspectName) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getAspectSpec", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get aspect models", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    if (systemOperationContext.getEntityRegistry().getAspectSpecs().get(aspectName) != null) {

      return ResponseEntity.ok(
          AspectSpecDto.fromAspectSpec(
              systemOperationContext.getEntityRegistry().getAspectSpecs().get(aspectName)));

    } else {
      return ResponseEntity.notFound().build();
    }
  }

  @GetMapping(path = "/event/specifications", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description = "Retrieves all event specs. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned event specs",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the entity registry")
      })
  public ResponseEntity<PaginatedResponse<EventSpecDto>> getEventSpecs(
      HttpServletRequest request,
      @Parameter(description = "Start index for pagination", example = "0")
          @RequestParam(name = "start", required = false, defaultValue = "0")
          Integer start,
      @Parameter(name = "count", description = "Number of items to return", example = "20")
          @RequestParam(name = "count", required = false, defaultValue = "20")
          Integer count) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getEventSpecs", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get event models", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    Map<String, EventSpec> eventSpecs = systemOperationContext.getEntityRegistry().getEventSpecs();

    // Use the PaginatedResponse helper to handle pagination and sorting
    PaginatedResponse<EventSpecDto> response =
        PaginatedResponse.fromMap(
            eventSpecs,
            start,
            count,
            entry -> {
              EventSpecDto dto = EventSpecDto.fromEventSpec(entry.getValue());
              // Ensure the name is set from the map key
              if (dto.getName() == null) {
                dto.setName(entry.getKey());
              }
              return dto;
            });

    return ResponseEntity.ok(response);
  }
}
