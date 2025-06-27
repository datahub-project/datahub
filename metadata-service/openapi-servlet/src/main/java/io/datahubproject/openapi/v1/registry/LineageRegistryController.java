package io.datahubproject.openapi.v1.registry;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/registry/lineage")
@Slf4j
@Tag(name = "Lineage Registry API", description = "An API to expose the Lineage Registry")
@AllArgsConstructor
@NoArgsConstructor
public class LineageRegistryController {
  @Autowired private AuthorizerChain authorizerChain;
  @Autowired private OperationContext systemOperationContext;

  @GetMapping(path = "/specifications", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description = "Retrieves all lineage specs. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the lineage registry")
      })
  public ResponseEntity<Map<String, LineageRegistry.LineageSpec>> getLineageSpecs(
      HttpServletRequest request) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getLineageSpecs", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get lineage", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(systemOperationContext.getLineageRegistry().getLineageSpecs());
  }

  @GetMapping(path = "/specifications/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves lineage spec for entity. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the lineage registry")
      })
  public ResponseEntity<LineageRegistry.LineageSpec> getLineageSpec(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getLineageSpec", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get lineage", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(
        systemOperationContext.getLineageRegistry().getLineageSpec(entityName));
  }

  @GetMapping(path = "/edges/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves lineage lineage edges for entity. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage edges",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the lineage registry")
      })
  public ResponseEntity<List<LineageRegistry.EdgeInfo>> getLineageEdges(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getLineageEdges", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get lineage", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    List<LineageRegistry.EdgeInfo> edges =
        Arrays.stream(LineageDirection.values())
            .flatMap(
                dir ->
                    systemOperationContext
                        .getLineageRegistry()
                        .getLineageRelationships(entityName, dir)
                        .stream())
            .distinct()
            .toList();

    return ResponseEntity.ok(edges);
  }

  @GetMapping(path = "/edges/{entityName}/{direction}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves lineage lineage edges for entity in the provided direction. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage edges",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access the lineage registry")
      })
  public ResponseEntity<List<LineageRegistry.EdgeInfo>> getLineageDirectedEdges(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("direction") LineageDirection direction) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr, request, "getLineageDirectedEdges", Collections.emptyList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get lineage", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    List<LineageRegistry.EdgeInfo> edges =
        systemOperationContext.getLineageRegistry().getLineageRelationships(entityName, direction);

    return ResponseEntity.ok(edges);
  }
}
