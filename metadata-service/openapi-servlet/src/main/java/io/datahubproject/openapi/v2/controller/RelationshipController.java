package io.datahubproject.openapi.v2.controller;

import static io.datahubproject.openapi.v2.utils.ControllerUtil.checkAuthorized;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.graph.RelatedEntities;
import com.linkedin.metadata.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import io.datahubproject.openapi.v2.models.GenericScrollResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v2/relationship")
@Slf4j
@Tag(
    name = "Generic Relationships",
    description = "APIs for ingesting and accessing entity relationships.")
public class RelationshipController {

  private static final String[] SORT_FIELDS = {"source.urn", "destination.urn", "relationshipType"};
  private static final String[] SORT_ORDERS = {"ASCENDING", "ASCENDING", "ASCENDING"};
  private static final List<SortCriterion> EDGE_SORT_CRITERION;

  static {
    EDGE_SORT_CRITERION =
        IntStream.range(0, SORT_FIELDS.length)
            .mapToObj(
                idx -> SearchUtil.sortBy(SORT_FIELDS[idx], SortOrder.valueOf(SORT_ORDERS[idx])))
            .collect(Collectors.toList());
  }

  @Autowired private EntityRegistry entityRegistry;
  @Autowired private ElasticSearchGraphService graphService;
  @Autowired private AuthorizerChain authorizationChain;

  @Autowired private boolean restApiAuthorizationEnabled;

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
      @PathVariable("relationshipType") String relationshipType,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId) {

    RelatedEntitiesScrollResult result =
        graphService.scrollRelatedEntities(
            null,
            null,
            null,
            null,
            List.of(relationshipType),
            new RelationshipFilter().setDirection(RelationshipDirection.UNDIRECTED),
            EDGE_SORT_CRITERION,
            scrollId,
            count,
            null,
            null);

    if (restApiAuthorizationEnabled) {
      Authentication authentication = AuthenticationContext.getAuthentication();
      Set<EntitySpec> entitySpecs =
          result.getEntities().stream()
              .flatMap(
                  relatedEntity ->
                      Stream.of(
                          entityRegistry.getEntitySpec(
                              UrnUtils.getUrn(relatedEntity.getUrn()).getEntityType()),
                          entityRegistry.getEntitySpec(
                              UrnUtils.getUrn(relatedEntity.getSourceUrn()).getEntityType())))
              .collect(Collectors.toSet());

      checkAuthorized(
          authorizationChain,
          authentication.getActor(),
          entitySpecs,
          ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE.getType()));
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
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @RequestParam(value = "relationshipType[]", required = false, defaultValue = "*")
          String[] relationshipTypes,
      @RequestParam(value = "direction", defaultValue = "OUTGOING") String direction,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "scrollId", required = false) String scrollId) {

    final RelatedEntitiesScrollResult result;

    switch (RelationshipDirection.valueOf(direction.toUpperCase())) {
      case INCOMING -> result =
          graphService.scrollRelatedEntities(
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
              EDGE_SORT_CRITERION,
              scrollId,
              count,
              null,
              null);
      case OUTGOING -> result =
          graphService.scrollRelatedEntities(
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
              EDGE_SORT_CRITERION,
              scrollId,
              count,
              null,
              null);
      default -> throw new IllegalArgumentException("Direction must be INCOMING or OUTGOING");
    }

    if (restApiAuthorizationEnabled) {
      Authentication authentication = AuthenticationContext.getAuthentication();
      Set<EntitySpec> entitySpecs =
          result.getEntities().stream()
              .flatMap(
                  relatedEntity ->
                      Stream.of(
                          entityRegistry.getEntitySpec(
                              UrnUtils.getUrn(relatedEntity.getDestinationUrn()).getEntityType()),
                          entityRegistry.getEntitySpec(
                              UrnUtils.getUrn(relatedEntity.getSourceUrn()).getEntityType())))
              .collect(Collectors.toSet());

      checkAuthorized(
          authorizationChain,
          authentication.getActor(),
          entitySpecs,
          ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE.getType()));
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
