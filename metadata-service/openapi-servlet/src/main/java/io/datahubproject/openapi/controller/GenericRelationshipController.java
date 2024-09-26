package io.datahubproject.openapi.controller;

import static com.linkedin.metadata.authorization.ApiGroup.RELATIONSHIP;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

public abstract class GenericRelationshipController {

  @Autowired private EntityRegistry entityRegistry;
  @Autowired private ElasticSearchGraphService graphService;
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
          Boolean includeSoftDelete) {

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
            .withSearchFlags(f -> f.setIncludeSoftDeleted(includeSoftDelete));

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
            null,
            null,
            null,
            List.of(relationshipType),
            new RelationshipFilter().setDirection(RelationshipDirection.UNDIRECTED),
            Edge.EDGE_SORT_CRITERION,
            scrollId,
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
          Boolean includeSoftDelete) {

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
            .withSearchFlags(f -> f.setIncludeSoftDeleted(includeSoftDelete));

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
              null,
              null,
              null,
              relationshipTypes.length > 0 && !relationshipTypes[0].equals("*")
                  ? Arrays.stream(relationshipTypes).toList()
                  : List.of(),
              new RelationshipFilter()
                  .setDirection(RelationshipDirection.UNDIRECTED)
                  .setOr(QueryUtils.newFilter("destination.urn", entityUrn).getOr()),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              count,
              null,
              null);
      case OUTGOING -> result =
          graphService.scrollRelatedEntities(
              opContext,
              null,
              null,
              null,
              null,
              relationshipTypes.length > 0 && !relationshipTypes[0].equals("*")
                  ? Arrays.stream(relationshipTypes).toList()
                  : List.of(),
              new RelationshipFilter()
                  .setDirection(RelationshipDirection.UNDIRECTED)
                  .setOr(QueryUtils.newFilter("source.urn", entityUrn).getOr()),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
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
