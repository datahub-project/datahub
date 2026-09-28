package io.swagger.api;

import io.datahubproject.schema_registry.openapi.generated.Association;
import io.datahubproject.schema_registry.openapi.generated.AssociationBatchResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Confluent Schema Registry Stream Governance Associations API.
 *
 * <p>Newer clients probe these endpoints even when no associations exist. INTERNAL registry
 * implementations should return empty GET lists rather than 404.
 */
@Validated
public interface AssociationsApi {

  @Operation(
      summary = "Get a list of associations by resource name",
      tags = {"Associations (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of associations",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema())))
      })
  @RequestMapping(
      value = "/associations/resources/{resourceNamespace}/{resourceName}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  ResponseEntity<List<Association>> getAssociationsByResourceName(
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("resourceNamespace")
          String resourceNamespace,
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("resourceName")
          String resourceName,
      @Valid @RequestParam(value = "resourceType", required = false) String resourceType,
      @Valid @RequestParam(value = "associationType", required = false)
          List<String> associationType,
      @Valid @RequestParam(value = "lifecycle", required = false) String lifecycle,
      @Valid @RequestParam(value = "offset", required = false) Integer offset,
      @Valid @RequestParam(value = "limit", required = false) Integer limit);

  @Operation(
      summary = "Get a list of associations by resource ID",
      tags = {"Associations (v1)"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "List of associations")})
  @RequestMapping(
      value = "/associations/resources/{resourceId}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  ResponseEntity<List<Association>> getAssociationsByResourceId(
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("resourceId")
          String resourceId,
      @Valid @RequestParam(value = "resourceType", required = false) String resourceType,
      @Valid @RequestParam(value = "associationType", required = false)
          List<String> associationType,
      @Valid @RequestParam(value = "lifecycle", required = false) String lifecycle,
      @Valid @RequestParam(value = "offset", required = false) Integer offset,
      @Valid @RequestParam(value = "limit", required = false) Integer limit);

  @Operation(
      summary = "Get a list of associations by subject",
      tags = {"Associations (v1)"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "List of associations")})
  @RequestMapping(
      value = "/associations/subjects/{subject}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  ResponseEntity<List<Association>> getAssociationsBySubject(
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("subject") String subject,
      @Valid @RequestParam(value = "resourceType", required = false) String resourceType,
      @Valid @RequestParam(value = "associationType", required = false)
          List<String> associationType,
      @Valid @RequestParam(value = "lifecycle", required = false) String lifecycle,
      @Valid @RequestParam(value = "offset", required = false) Integer offset,
      @Valid @RequestParam(value = "limit", required = false) Integer limit);

  @Operation(
      summary = "Create an association",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      consumes = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream"
      },
      method = RequestMethod.POST)
  ResponseEntity<Void> createAssociation(
      @Valid @RequestBody Map<String, Object> body,
      @Valid @RequestParam(value = "context", required = false) String context,
      @Valid @RequestParam(value = "dryRun", required = false) Boolean dryRun);

  @Operation(
      summary = "Create or update an association",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations/resources/{resourceId}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      consumes = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream"
      },
      method = RequestMethod.PUT)
  ResponseEntity<Void> createOrUpdateAssociation(
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("resourceId")
          String resourceId,
      @Valid @RequestBody Map<String, Object> body,
      @Valid @RequestParam(value = "context", required = false) String context,
      @Valid @RequestParam(value = "dryRun", required = false) Boolean dryRun);

  @Operation(
      summary = "Delete associations",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations/resources/{resourceId}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.DELETE)
  ResponseEntity<Void> deleteAssociations(
      @Parameter(in = ParameterIn.PATH, required = true) @PathVariable("resourceId")
          String resourceId,
      @Valid @RequestParam(value = "resourceType", required = false) String resourceType,
      @Valid @RequestParam(value = "associationType", required = false)
          List<String> associationType,
      @Valid @RequestParam(value = "cascadeLifecycle", required = false) Boolean cascadeLifecycle,
      @Valid @RequestParam(value = "dryRun", required = false) Boolean dryRun);

  @Operation(
      summary = "Mutate associations in batch",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations:batch",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      consumes = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream"
      },
      method = RequestMethod.POST)
  ResponseEntity<Void> mutateAssociations(
      @Valid @RequestBody(required = false) Map<String, Object> body,
      @Valid @RequestParam(value = "context", required = false) String context,
      @Valid @RequestParam(value = "dryRun", required = false) Boolean dryRun);

  @Operation(
      summary = "Mutate associations in batch (Confluent RestService path)",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations:batchMutate",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      consumes = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream"
      },
      method = RequestMethod.POST)
  ResponseEntity<Void> mutateAssociationsAlias(
      @Valid @RequestBody(required = false) Map<String, Object> body,
      @Valid @RequestParam(value = "context", required = false) String context,
      @Valid @RequestParam(value = "dryRun", required = false) Boolean dryRun);

  @Operation(
      summary = "Get associations in batch",
      tags = {"Associations (v1)"})
  @RequestMapping(
      value = "/associations:batchGet",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      consumes = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
        "application/octet-stream"
      },
      method = RequestMethod.POST)
  ResponseEntity<AssociationBatchResponse> batchGetAssociations(
      @Valid @RequestBody(required = false) Map<String, Object> body,
      @Valid @RequestParam(value = "includeSchemas", required = false) Boolean includeSchemas);
}
