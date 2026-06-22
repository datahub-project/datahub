package io.datahubproject.openapi.v3.controller;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.controller.ScrollUtils;
import io.datahubproject.openapi.models.GenericScrollResult;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import io.datahubproject.openapi.v3.models.ScrollRelationshipsRequestBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("LineageControllerV3")
@RequiredArgsConstructor
@RequestMapping("/openapi/v3/lineage")
@Slf4j
@Tag(name = "Generic Lineage", description = "APIs for accessing lineage relationships.")
public class LineageController {

  @Autowired private GraphService graphService;
  @Autowired private AuthorizerChain authorizationChain;

  @Qualifier("systemOperationContext")
  @Autowired
  private OperationContext systemOperationContext;

  /**
   * Scrolls lineage relationship edges. Applies a triplet-based lineage filter from the entity
   * registry so that only edges annotated as lineage are returned. Callers can still layer on
   * source/destination/relationship type filters, entity filters, and direction as needed.
   */
  @PostMapping(value = "/scroll", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Scroll lineage relationships with configurable filters on source/destination types and edges.")
  public ResponseEntity<GenericScrollResult<GenericRelationship>> scrollLineage(
      HttpServletRequest request,
      @RequestParam(value = "relationshipTypes", required = false) String[] relationshipTypes,
      @RequestParam(value = "sourceTypes", required = false) String[] sourceTypes,
      @RequestParam(value = "destinationTypes", required = false) String[] destinationTypes,
      @RequestParam(value = "direction", defaultValue = "OUTGOING") String direction,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive,
      @RequestBody @Nonnull ScrollRelationshipsRequestBody body) {
    GraphFilters baseFilters = GraphFilters.forLineage(graphService.getLineageRegistry());
    return ScrollUtils.doScrollRelationships(
        systemOperationContext,
        authorizationChain,
        graphService,
        request,
        "scrollLineage",
        relationshipTypes,
        sourceTypes,
        destinationTypes,
        direction,
        count,
        scrollId,
        includeSoftDelete,
        sliceId,
        sliceMax,
        pitKeepAlive,
        body,
        baseFilters);
  }
}
