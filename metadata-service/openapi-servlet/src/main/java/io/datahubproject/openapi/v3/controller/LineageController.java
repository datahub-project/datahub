package io.datahubproject.openapi.v3.controller;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.controller.ScrollUtils;
import io.datahubproject.openapi.models.GenericScrollResult;
import io.datahubproject.openapi.v3.models.LineageRelationship;
import io.datahubproject.openapi.v3.models.ScrollLineageRequestBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
   * Scrolls lineage relationship edges connected to a set of URNs. URNs are matched against either
   * endpoint of an edge (source or destination) — the graph is always queried undirected. Applies a
   * triplet-based lineage filter from the entity registry so that only edges annotated as lineage
   * are returned.
   *
   * <p>When {@code urns} is omitted or empty, no URN filter is applied and all lineage edges are
   * scrolled.
   *
   * <p>{@code direction} (UPSTREAM/DOWNSTREAM) is applied as a post-query filter: after fetching
   * undirected matches, edges are classified via the {@link LineageRegistry} relative to {@code
   * urns} and edges running the opposite way are dropped. Because of this, a given page may return
   * fewer than {@code count} results — callers must keep scrolling until {@code scrollId} is null.
   * {@code direction} has no effect when {@code urns} is empty, since there is no anchor to orient
   * from.
   */
  @PostMapping(value = "/scroll", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Scroll lineage relationship edges connected to a set of URNs (matched on either "
              + "endpoint), optionally restricted to upstream or downstream edges relative to "
              + "those URNs.")
  public ResponseEntity<GenericScrollResult<LineageRelationship>> scrollLineage(
      HttpServletRequest request,
      @RequestParam(value = "relationshipTypes", required = false) String[] relationshipTypes,
      @RequestParam(value = "direction", required = false) String direction,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive,
      @RequestBody @Nonnull ScrollLineageRequestBody body) {
    LineageDirection lineageDirection = parseLineageDirection(direction);

    return ScrollUtils.doScrollLineage(
        systemOperationContext,
        authorizationChain,
        graphService,
        request,
        "scrollLineage",
        UsageOperation.LINEAGE_QUERY,
        relationshipTypes,
        body.getUrns(),
        lineageDirection,
        count,
        scrollId,
        includeSoftDelete,
        sliceId,
        sliceMax,
        pitKeepAlive);
  }

  @Nullable
  private static LineageDirection parseLineageDirection(@Nullable String direction) {
    if (direction == null || direction.isEmpty()) {
      return null;
    }
    try {
      return LineageDirection.valueOf(direction.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "direction must be UPSTREAM or DOWNSTREAM, got: " + direction);
    }
  }
}
