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
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.models.GenericScrollResult;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

public abstract class GenericRelationshipController {

  @Autowired private EntityRegistry entityRegistry;
  @Autowired private GraphService graphService;
  @Autowired private AuthorizerChain authorizationChain;

  @Qualifier("systemOperationContext")
  @Autowired
  protected OperationContext systemOperationContext;

  /**
   * Returns relationship edges by type
   *
   * @param relationshipType the relationship type
   * @param count number of results
   * @param scrollId scrolling id
   * @return list of relation edges
   */
  @GetMapping(value = "/{relationshipType}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Scroll relationships of the given type.")
  public ResponseEntity<GenericScrollResult<GenericRelationship>> getRelationshipsByType(
      HttpServletRequest request,
      @PathVariable("relationshipType") String relationshipType,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi(
                        authentication.getActor().toUrnStr(),
                        request,
                        "getRelationshipsByType",
                        List.of()),
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

    if (!AuthUtil.isAPIAuthorized(opContext, RELATIONSHIP, READ)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    RelatedEntitiesScrollResult result =
        graphService.scrollRelatedEntities(
            opContext,
            null,
            QueryUtils.EMPTY_FILTER,
            null,
            QueryUtils.EMPTY_FILTER,
            Set.of(relationshipType),
            QueryUtils.newRelationshipFilter(
                QueryUtils.EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
            Edge.EDGE_SORT_CRITERION,
            scrollId,
            pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
            count,
            null,
            null);

    if (!AuthUtil.isAPIAuthorizedUrns(
        opContext,
        RELATIONSHIP,
        READ,
        result.getEntities().stream()
            .flatMap(
                edge ->
                    Stream.of(
                        UrnUtils.getUrn(edge.getSourceUrn()),
                        UrnUtils.getUrn(edge.getDestinationUrn())))
            .collect(Collectors.toSet()))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    return ResponseEntity.ok(
        GenericScrollResult.<GenericRelationship>builder()
            .results(toGenericRelationships(result.getEntities()))
            .scrollId(result.getScrollId())
            .build());
  }

  /**
   * Returns edges for a given urn
   *
   * @param relationshipTypes types of edges
   * @param direction direction of the edges
   * @param count number of results
   * @param scrollId scroll id
   * @return urn edges
   */
  @GetMapping(value = "/{entityName}/{entityUrn}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Scroll relationships from a given entity.")
  public ResponseEntity<GenericScrollResult<GenericRelationship>> getRelationshipsByEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @RequestParam(value = "relationshipType[]", required = false, defaultValue = "*")
          String[] relationshipTypes,
      @RequestParam(value = "direction", defaultValue = "OUTGOING") String direction,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive) {

    final RelatedEntitiesScrollResult result;

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi(
                        authentication.getActor().toUrnStr(),
                        request,
                        "getRelationshipsByEntity",
                        List.of()),
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

    if (!AuthUtil.isAPIAuthorizedUrns(
        opContext, RELATIONSHIP, READ, List.of(UrnUtils.getUrn(entityUrn)))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    switch (RelationshipDirection.valueOf(direction.toUpperCase())) {
      case INCOMING -> result =
          graphService.scrollRelatedEntities(
              opContext,
              null,
              QueryUtils.EMPTY_FILTER,
              null,
              QueryUtils.newFilter("urn", entityUrn),
              relationshipTypes.length > 0 && !relationshipTypes[0].equals("*")
                  ? Arrays.stream(relationshipTypes).collect(Collectors.toSet())
                  : Set.of(),
              QueryUtils.newRelationshipFilter(
                  QueryUtils.EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
              count,
              null,
              null);
      case OUTGOING -> result =
          graphService.scrollRelatedEntities(
              opContext,
              null,
              QueryUtils.newFilter("urn", entityUrn),
              null,
              QueryUtils.EMPTY_FILTER,
              relationshipTypes.length > 0 && !relationshipTypes[0].equals("*")
                  ? Arrays.stream(relationshipTypes).collect(Collectors.toSet())
                  : Set.of(),
              QueryUtils.newRelationshipFilter(
                  QueryUtils.EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
              count,
              null,
              null);
      default -> throw new IllegalArgumentException("Direction must be INCOMING or OUTGOING");
    }

    if (!AuthUtil.isAPIAuthorizedUrns(
        opContext,
        RELATIONSHIP,
        READ,
        result.getEntities().stream()
            .flatMap(
                edge ->
                    Stream.of(
                        UrnUtils.getUrn(edge.getSourceUrn()),
                        UrnUtils.getUrn(edge.getDestinationUrn())))
            .collect(Collectors.toSet()))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    return ResponseEntity.ok(
        GenericScrollResult.<GenericRelationship>builder()
            .results(toGenericRelationships(result.getEntities()))
            .scrollId(result.getScrollId())
            .build());
  }

  /**
   * Scrolls relationships with configurable filters on source/destination entity types and URNs.
   *
   * @param relationshipTypes relationship types to filter on (default all)
   * @param sourceTypes entity types to filter on for source
   * @param destinationTypes entity types to filter on for destination
   * @param sourceUrns URNs to filter on for source (OR logic)
   * @param destinationUrns URNs to filter on for destination (OR logic)
   * @param count number of results
   * @param scrollId scrolling id
   * @return list of relation edges
   */
  @GetMapping(value = "/scroll", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Scroll relationships with configurable filters on source/destination types and URNs.")
  public ResponseEntity<GenericScrollResult<GenericRelationship>> scrollRelationships(
      HttpServletRequest request,
      @RequestParam(value = "relationshipTypes", required = false) String[] relationshipTypes,
      @RequestParam(value = "sourceTypes", required = false) String[] sourceTypes,
      @RequestParam(value = "destinationTypes", required = false) String[] destinationTypes,
      @RequestParam(value = "sourceUrns", required = false) String[] sourceUrns,
      @RequestParam(value = "destinationUrns", required = false) String[] destinationUrns,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi(
                        authentication.getActor().toUrnStr(),
                        request,
                        "scrollRelationships",
                        List.of()),
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

    if (!AuthUtil.isAPIAuthorized(opContext, RELATIONSHIP, READ)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    Set<String> sourceTypesSet =
        sourceTypes != null && sourceTypes.length > 0
            ? Arrays.stream(sourceTypes).collect(Collectors.toSet())
            : null;
    Set<String> destinationTypesSet =
        destinationTypes != null && destinationTypes.length > 0
            ? Arrays.stream(destinationTypes).collect(Collectors.toSet())
            : null;

    Filter sourceEntityFilter =
        sourceUrns != null && sourceUrns.length > 0
            ? QueryUtils.newFilter(QueryUtils.newCriterion("urn", Arrays.asList(sourceUrns)))
            : QueryUtils.EMPTY_FILTER;
    Filter destinationEntityFilter =
        destinationUrns != null && destinationUrns.length > 0
            ? QueryUtils.newFilter(QueryUtils.newCriterion("urn", Arrays.asList(destinationUrns)))
            : QueryUtils.EMPTY_FILTER;

    RelatedEntitiesScrollResult result =
        graphService.scrollRelatedEntities(
            opContext,
            sourceTypesSet,
            sourceEntityFilter,
            destinationTypesSet,
            destinationEntityFilter,
            relationshipTypes != null
                ? Arrays.stream(relationshipTypes).collect(Collectors.toSet())
                : Set.of(),
            QueryUtils.newRelationshipFilter(
                QueryUtils.EMPTY_FILTER, RelationshipDirection.UNDIRECTED),
            Edge.EDGE_SORT_CRITERION,
            scrollId,
            pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
            count,
            null,
            null);

    if (!AuthUtil.isAPIAuthorizedUrns(
        opContext,
        RELATIONSHIP,
        READ,
        result.getEntities().stream()
            .flatMap(
                edge ->
                    Stream.of(
                        UrnUtils.getUrn(edge.getSourceUrn()),
                        UrnUtils.getUrn(edge.getDestinationUrn())))
            .collect(Collectors.toSet()))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " "
              + RELATIONSHIP);
    }

    return ResponseEntity.ok(
        GenericScrollResult.<GenericRelationship>builder()
            .results(toGenericRelationships(result.getEntities()))
            .scrollId(result.getScrollId())
            .build());
  }

  private List<GenericRelationship> toGenericRelationships(List<RelatedEntities> relatedEntities) {
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
}
