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
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.models.GenericScrollResult;
import io.datahubproject.openapi.v2.models.GenericRelationship;
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
   * Shared implementation for scroll endpoints. When {@code baseFilters} is non-null its filters
   * (e.g. lineage triplets) are applied additively to the filters derived from the request
   * parameters.
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
      ScrollRelationshipsRequestBody body,
      @Nullable GraphFilters baseFilters) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
                systemOperationContext,
                RequestContext.builder()
                    .buildOpenapi(
                        authentication.getActor().toUrnStr(), request, operationName, List.of()),
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

    com.linkedin.metadata.query.filter.Filter sourceEntityFilter =
        Optional.ofNullable(body.getSourceFilter())
            .map(io.datahubproject.openapi.v3.models.Filter::toRecordTemplate)
            .orElse(QueryUtils.EMPTY_FILTER);
    com.linkedin.metadata.query.filter.Filter destinationEntityFilter =
        Optional.ofNullable(body.getDestinationFilter())
            .map(io.datahubproject.openapi.v3.models.Filter::toRecordTemplate)
            .orElse(QueryUtils.EMPTY_FILTER);
    com.linkedin.metadata.query.filter.Filter edgeFilter =
        Optional.ofNullable(body.getEdgeFilter())
            .map(io.datahubproject.openapi.v3.models.Filter::toRecordTemplate)
            .orElse(QueryUtils.EMPTY_FILTER);

    RelationshipDirection relationshipDirection;
    try {
      relationshipDirection = RelationshipDirection.valueOf(direction.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Direction must be INCOMING, OUTGOING, or UNDIRECTED, got: " + direction);
    }

    // Build GraphFilters from the request parameters.
    GraphFilters graphFilters =
        new GraphFilters(
            sourceEntityFilter,
            destinationEntityFilter,
            sourceTypesSet,
            destinationTypesSet,
            relationshipTypes != null
                ? Arrays.stream(relationshipTypes).collect(Collectors.toSet())
                : Set.of(),
            QueryUtils.newRelationshipFilter(edgeFilter, relationshipDirection));

    // Merge in base filters (e.g. lineage triplets) if provided.
    if (baseFilters != null && baseFilters.getAllowedEdgeTriplets() != null) {
      graphFilters.setAllowedEdgeTriplets(baseFilters.getAllowedEdgeTriplets());
    }

    RelatedEntitiesScrollResult result =
        graphService.scrollRelatedEntities(
            opContext,
            graphFilters,
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
}
