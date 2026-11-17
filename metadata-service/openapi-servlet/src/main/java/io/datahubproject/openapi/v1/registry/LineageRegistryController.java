package io.datahubproject.openapi.v1.registry;

import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
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
@Tag(name = "Lineage Registry API", description = "An API to expose the Lineage Registry")
@AllArgsConstructor
@NoArgsConstructor
public class LineageRegistryController {
  @Autowired private RegistryApiAuthorization registryApiAuthorization;
  @Autowired private OperationContext systemOperationContext;

  @GetMapping(path = "/specifications", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves all lineage specs." + RegistryApiAuthorization.READ_ACCESS_DESCRIPTION,
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(responseCode = "401", description = "Authentication required"),
        @ApiResponse(
            responseCode = "403",
            description = RegistryApiAuthorization.FORBIDDEN_DESCRIPTION)
      })
  public ResponseEntity<Map<String, LineageRegistry.LineageSpec>> getLineageSpecs(
      HttpServletRequest request) {

    Optional<HttpStatus> denialStatus =
        registryApiAuthorization.getReadDenialStatus(request, "getLineageSpecs");
    if (denialStatus.isPresent()) {
      return ResponseEntity.status(denialStatus.get()).build();
    }

    return ResponseEntity.ok(systemOperationContext.getLineageRegistry().getLineageSpecs());
  }

  @GetMapping(path = "/specifications/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves lineage spec for entity." + RegistryApiAuthorization.READ_ACCESS_DESCRIPTION,
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage spec",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(responseCode = "401", description = "Authentication required"),
        @ApiResponse(
            responseCode = "403",
            description = RegistryApiAuthorization.FORBIDDEN_DESCRIPTION)
      })
  public ResponseEntity<LineageRegistry.LineageSpec> getLineageSpec(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Optional<HttpStatus> denialStatus =
        registryApiAuthorization.getReadDenialStatus(request, "getLineageSpec");
    if (denialStatus.isPresent()) {
      return ResponseEntity.status(denialStatus.get()).build();
    }

    return ResponseEntity.ok(
        systemOperationContext.getLineageRegistry().getLineageSpec(entityName));
  }

  @GetMapping(path = "/edges/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves lineage edges for entity." + RegistryApiAuthorization.READ_ACCESS_DESCRIPTION,
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage edges",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(responseCode = "401", description = "Authentication required"),
        @ApiResponse(
            responseCode = "403",
            description = RegistryApiAuthorization.FORBIDDEN_DESCRIPTION)
      })
  public ResponseEntity<List<LineageRegistry.EdgeInfo>> getLineageEdges(
      HttpServletRequest request, @PathVariable("entityName") String entityName) {

    Optional<HttpStatus> denialStatus =
        registryApiAuthorization.getReadDenialStatus(request, "getLineageEdges");
    if (denialStatus.isPresent()) {
      return ResponseEntity.status(denialStatus.get()).build();
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
          "Retrieves lineage edges for entity in the provided direction."
              + RegistryApiAuthorization.READ_ACCESS_DESCRIPTION,
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully returned lineage edges",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(responseCode = "401", description = "Authentication required"),
        @ApiResponse(
            responseCode = "403",
            description = RegistryApiAuthorization.FORBIDDEN_DESCRIPTION)
      })
  public ResponseEntity<List<LineageRegistry.EdgeInfo>> getLineageDirectedEdges(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("direction") LineageDirection direction) {

    Optional<HttpStatus> denialStatus =
        registryApiAuthorization.getReadDenialStatus(request, "getLineageDirectedEdges");
    if (denialStatus.isPresent()) {
      return ResponseEntity.status(denialStatus.get()).build();
    }

    List<LineageRegistry.EdgeInfo> edges =
        systemOperationContext.getLineageRegistry().getLineageRelationships(entityName, direction);

    return ResponseEntity.ok(edges);
  }
}
