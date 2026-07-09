package io.datahubproject.openapi.controller;

import static com.linkedin.metadata.authorization.ApiGroup.RELATIONSHIP;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.models.GenericScrollResult;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import io.datahubproject.openapi.v3.models.LineageRelationship;
import io.datahubproject.openapi.v3.models.ScrollRelationshipsRequestBody;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.http.ResponseEntity;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ScrollUtils {

  /**
   * Scrolls generic relationship edges filtered by independent source/destination type and entity
   * filters.
   */
  public static ResponseEntity<GenericScrollResult<GenericRelationship>> doScrollRelationships(
      OperationContext systemOperationContext,
      AuthorizerChain authorizationChain,
      GraphService graphService,
      HttpServletRequest request,
      String operationName,
      String[] relationshipTypes,
      String[] sourceTypes,
      String[] destinationTypes,
      String direction,
      Integer count,
      String scrollId,
      Boolean includeSoftDelete,
      Integer sliceId,
      Integer sliceMax,
      String pitKeepAlive,
      ScrollRelationshipsRequestBody body) {

    OperationContext opContext =
        getOpContext(
            systemOperationContext,
            authorizationChain,
            request,
            operationName,
            UsageOperation.METADATA_READ,
            includeSoftDelete,
            sliceId,
            sliceMax);
    checkAuthorized(opContext);

    RelationshipDirection relationshipDirection = parseRelationshipDirection(direction);

    GraphFilters graphFilters =
        new GraphFilters(
            toFilter(body.getSourceFilter()),
            toFilter(body.getDestinationFilter()),
            toOptionalTypesSet(sourceTypes),
            toOptionalTypesSet(destinationTypes),
            toRelationshipTypesSet(relationshipTypes),
            QueryUtils.newRelationshipFilter(
                toFilter(body.getEdgeFilter()), relationshipDirection));

    RelatedEntitiesScrollResult result =
        scrollRelatedEntities(graphService, opContext, graphFilters, scrollId, pitKeepAlive, count);

    Set<Urn> resultUrns =
        result.getEntities().stream()
            .flatMap(
                edge ->
                    Stream.of(
                        UrnUtils.getUrn(edge.getSourceUrn()),
                        UrnUtils.getUrn(edge.getDestinationUrn())))
            .collect(Collectors.toSet());
    return ResponseEntity.ok(
        GenericScrollResult.<GenericRelationship>builder()
            .results(toGenericRelationships(result.getEntities()))
            .scrollId(result.getScrollId())
            .build());
  }

  static List<GenericRelationship> toGenericRelationships(List<RelatedEntities> relatedEntities) {
    return relatedEntities.stream()
        .map(
            result -> {
              Urn source = UrnUtils.getUrn(result.getSourceUrn());
              Urn dest = UrnUtils.getUrn(result.getDestinationUrn());
              return GenericRelationship.builder()
                  .relationshipType(result.getRelationshipType())
                  .source(GenericRelationship.GenericNode.fromUrn(source))
                  .destination(GenericRelationship.GenericNode.fromUrn(dest))
                  .build();
            })
        .collect(Collectors.toList());
  }

  /**
   * Scrolls lineage relationship edges connected to {@code urns} (matched on either endpoint) — the
   * graph is always queried undirected. Applies a triplet-based lineage filter from the entity
   * registry so that only edges annotated as lineage are returned.
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
  public static ResponseEntity<GenericScrollResult<LineageRelationship>> doScrollLineage(
      OperationContext systemOperationContext,
      AuthorizerChain authorizationChain,
      GraphService graphService,
      HttpServletRequest request,
      String operationName,
      String[] relationshipTypes,
      @Nullable List<String> urns,
      @Nullable LineageDirection direction,
      Integer count,
      String scrollId,
      Boolean includeSoftDelete,
      Integer sliceId,
      Integer sliceMax,
      String pitKeepAlive) {

    OperationContext opContext =
        getOpContext(
            systemOperationContext,
            authorizationChain,
            request,
            operationName,
            UsageOperation.LINEAGE_QUERY,
            includeSoftDelete,
            sliceId,
            sliceMax);
    checkAuthorized(opContext);

    LineageRegistry lineageRegistry = graphService.getLineageRegistry();

    GraphFilters graphFilters =
        new GraphFilters(
            QueryUtils.EMPTY_FILTER,
            QueryUtils.EMPTY_FILTER,
            null,
            null,
            toRelationshipTypesSet(relationshipTypes),
            QueryUtils.newRelationshipFilter(
                buildUrnEndpointFilter(urns), RelationshipDirection.UNDIRECTED));
    graphFilters.setAllowedEdgeTriplets(
        GraphFilters.forLineage(lineageRegistry).getAllowedEdgeTriplets());

    RelatedEntitiesScrollResult result =
        scrollRelatedEntities(graphService, opContext, graphFilters, scrollId, pitKeepAlive, count);

    // Label each edge with its upstream/downstream endpoints, then drop edges running the wrong
    // way relative to the anchor urns (a no-op when direction or urns are absent).
    List<LineageRelationship> lineage =
        toLineageRelationships(result.getEntities(), lineageRegistry).stream()
            .filter(edge -> keepByLineageDirection(edge, urns, direction))
            .collect(Collectors.toList());

    Set<Urn> resultUrns =
        lineage.stream()
            .flatMap(
                edge ->
                    Stream.of(
                        UrnUtils.getUrn(edge.getUpstream()), UrnUtils.getUrn(edge.getDownstream())))
            .collect(Collectors.toSet());
    checkAuthorizedOnUrns(opContext, resultUrns);

    return ResponseEntity.ok(
        GenericScrollResult.<LineageRelationship>builder()
            .results(lineage)
            .scrollId(result.getScrollId())
            .build());
  }

  private static OperationContext getOpContext(
      OperationContext systemOperationContext,
      AuthorizerChain authorizationChain,
      HttpServletRequest request,
      String operationName,
      UsageOperation usageOperation,
      Boolean includeSoftDelete,
      Integer sliceId,
      Integer sliceMax) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    return OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, operationName, List.of())
                .withUsageOperation(usageOperation),
            authorizationChain,
            authentication,
            true)
        .withSearchFlags(
            f ->
                f.setIncludeSoftDeleted(includeSoftDelete)
                    .setSliceOptions(
                        sliceId != null && sliceMax != null
                            ? new SliceOptions().setId(sliceId).setMax(sliceMax)
                            : null,
                        SetMode.IGNORE_NULL));
  }

  private static void checkAuthorized(OperationContext opContext) {
    if (!AuthUtil.isAPIAuthorized(opContext, RELATIONSHIP, READ)) {
      throw unauthorized();
    }
  }

  private static void checkAuthorizedOnUrns(OperationContext opContext, Set<Urn> urns) {
    if (!AuthUtil.isAPIAuthorizedUrns(opContext, RELATIONSHIP, READ, urns)) {
      throw unauthorized();
    }
  }

  private static UnauthorizedException unauthorized() {
    return new UnauthorizedException(
        AuthenticationContext.getAuthentication().getActor().toUrnStr()
            + " is unauthorized to "
            + READ
            + " "
            + RELATIONSHIP);
  }

  private static RelatedEntitiesScrollResult scrollRelatedEntities(
      GraphService graphService,
      OperationContext opContext,
      GraphFilters graphFilters,
      String scrollId,
      String pitKeepAlive,
      Integer count) {
    return graphService.scrollRelatedEntities(
        opContext,
        graphFilters,
        Edge.EDGE_SORT_CRITERION,
        scrollId,
        pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
        count,
        null,
        null);
  }

  private static Filter toFilter(@Nullable io.datahubproject.openapi.v3.models.Filter filter) {
    return Optional.ofNullable(filter)
        .map(io.datahubproject.openapi.v3.models.Filter::toRecordTemplate)
        .orElse(QueryUtils.EMPTY_FILTER);
  }

  private static RelationshipDirection parseRelationshipDirection(String direction) {
    try {
      return RelationshipDirection.valueOf(direction.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Direction must be INCOMING, OUTGOING, or UNDIRECTED, got: " + direction);
    }
  }

  @Nullable
  private static Set<String> toOptionalTypesSet(@Nullable String[] types) {
    return types != null && types.length > 0
        ? Arrays.stream(types).collect(Collectors.toSet())
        : null;
  }

  private static Set<String> toRelationshipTypesSet(@Nullable String[] relationshipTypes) {
    return relationshipTypes != null
        ? Arrays.stream(relationshipTypes).collect(Collectors.toSet())
        : Set.of();
  }

  /**
   * Builds an edge filter matching edges where {@code urns} appears as the source OR the
   * destination. Returns {@link QueryUtils#EMPTY_FILTER} (matches all edges) when {@code urns} is
   * null or empty.
   */
  static Filter buildUrnEndpointFilter(@Nullable List<String> urns) {
    if (urns == null || urns.isEmpty()) {
      return QueryUtils.EMPTY_FILTER;
    }
    Criterion sourceCriterion =
        CriterionUtils.buildCriterion(Edge.EDGE_SOURCE_URN_FIELD, Condition.EQUAL, urns);
    Criterion destinationCriterion =
        CriterionUtils.buildCriterion(Edge.EDGE_DESTINATION_URN_FIELD, Condition.EQUAL, urns);
    return QueryUtils.newDisjunctiveFilter(sourceCriterion, destinationCriterion);
  }

  /**
   * Keeps {@code edge} when it runs in {@code direction} relative to an anchor in {@code urns}:
   * UPSTREAM keeps edges whose downstream endpoint is an anchor (so the upstream endpoint is an
   * upstream of it), DOWNSTREAM keeps edges whose upstream endpoint is an anchor. Returns true (no
   * filtering) when {@code direction} or {@code urns} is absent.
   */
  static boolean keepByLineageDirection(
      LineageRelationship edge, @Nullable List<String> urns, @Nullable LineageDirection direction) {
    if (direction == null || urns == null || urns.isEmpty()) {
      return true;
    }
    return direction == LineageDirection.UPSTREAM
        ? urns.contains(edge.getDownstream())
        : urns.contains(edge.getUpstream());
  }

  private static boolean matchesRegisteredEdge(
      LineageRegistry registry,
      String entityType,
      LineageDirection lineageDirection,
      String relationshipType,
      RelationshipDirection relationshipDirection) {
    List<LineageRegistry.EdgeInfo> edges =
        registry.getLineageRelationships(entityType, lineageDirection);
    return edges.stream()
        .anyMatch(
            edgeInfo ->
                edgeInfo.getType().equals(relationshipType)
                    && edgeInfo.getDirection() == relationshipDirection);
  }

  /**
   * Resolves each edge's endpoints to lineage direction. For an edge stored as {@code source
   * --relationshipType--> destination}, the destination is the upstream when the source entity type
   * registers {@code (relationshipType, OUTGOING)} as an UPSTREAM lineage edge (e.g. DownstreamOf,
   * DerivedFrom, Consumes); otherwise the source is the upstream (e.g. Produces).
   */
  static List<LineageRelationship> toLineageRelationships(
      List<RelatedEntities> relatedEntities, LineageRegistry registry) {
    return relatedEntities.stream()
        .map(
            edge -> {
              Urn source = UrnUtils.getUrn(edge.getSourceUrn());
              Urn destination = UrnUtils.getUrn(edge.getDestinationUrn());
              boolean destinationIsUpstream =
                  matchesRegisteredEdge(
                      registry,
                      source.getEntityType(),
                      LineageDirection.UPSTREAM,
                      edge.getRelationshipType(),
                      RelationshipDirection.OUTGOING);
              return LineageRelationship.builder()
                  .relationshipType(edge.getRelationshipType())
                  .source(GenericRelationship.GenericNode.fromUrn(source))
                  .destination(GenericRelationship.GenericNode.fromUrn(destination))
                  .upstream((destinationIsUpstream ? destination : source).toString())
                  .downstream((destinationIsUpstream ? source : destination).toString())
                  .build();
            })
        .collect(Collectors.toList());
  }
}
