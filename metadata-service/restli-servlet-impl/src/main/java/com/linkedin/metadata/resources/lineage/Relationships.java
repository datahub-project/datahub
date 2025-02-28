package com.linkedin.metadata.resources.lineage;

import static com.datahub.authorization.AuthUtil.isAPIAuthorizedUrns;
import static com.linkedin.metadata.authorization.ApiGroup.LINEAGE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_COUNT;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_DIRECTION;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_START;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_URN;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** Rest.li entry point: /relationships?type={entityType}&direction={direction}&types={types} */
@Slf4j
@RestLiSimpleResource(name = "relationships", namespace = "com.linkedin.lineage")
public final class Relationships extends SimpleResourceTemplate<EntityRelationships> {

  private static final Integer MAX_DOWNSTREAM_CNT = 100;

  private static final String ACTION_GET_LINEAGE = "getLineage";
  private static final String PARAM_MAX_HOPS = "maxHops";

  @Inject
  @Named("graphService")
  private GraphService _graphService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  public Relationships() {
    super();
  }

  private RelatedEntitiesResult getRelatedEntities(
      String rawUrn,
      List<String> relationshipTypes,
      RelationshipDirection direction,
      @Nullable Integer start,
      @Nullable Integer count) {

    start = start == null ? 0 : start;
    count = count == null ? MAX_DOWNSTREAM_CNT : count;

    return _graphService.findRelatedEntities(systemOperationContext,
        null,
        newFilter("urn", rawUrn),
        null,
        QueryUtils.EMPTY_FILTER,
        relationshipTypes,
        newRelationshipFilter(QueryUtils.EMPTY_FILTER, direction),
        start,
        count);
  }

  static RelationshipDirection getOppositeDirection(RelationshipDirection direction) {
    if (direction.equals(RelationshipDirection.INCOMING)) {
      return RelationshipDirection.OUTGOING;
    }
    if (direction.equals(RelationshipDirection.OUTGOING)) {
      return RelationshipDirection.INCOMING;
    }
    return direction;
  }

  @Nonnull
  @RestMethod.Get
  @WithSpan
  public Task<EntityRelationships> get(
      @QueryParam("urn") @Nonnull String rawUrn,
      @QueryParam("types") @Nonnull String[] relationshipTypesParam,
      @QueryParam("direction") @Nonnull String rawDirection,
      @QueryParam("start") @Optional @Nullable Integer start,
      @QueryParam("count") @Optional @Nullable Integer count) {
    Urn urn = UrnUtils.getUrn(rawUrn);

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "getRelationships", urn.getEntityType()), _authorizer, auth, true);

    if (!isAPIAuthorizedUrns(
            opContext,
            LINEAGE, READ,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity lineage: " + rawUrn);
    }
    RelationshipDirection direction = RelationshipDirection.valueOf(rawDirection);
    final List<String> relationshipTypes = Arrays.asList(relationshipTypesParam);
    return RestliUtils.toTask(systemOperationContext,
        () -> {
          final RelatedEntitiesResult relatedEntitiesResult =
              getRelatedEntities(rawUrn, relationshipTypes, direction, start, count);
          final EntityRelationshipArray entityArray =
              new EntityRelationshipArray(
                  relatedEntitiesResult.getEntities().stream()
                      .map(
                          entity -> {
                            try {
                              return new EntityRelationship()
                                  .setEntity(Urn.createFromString(entity.getUrn()))
                                  .setType(entity.getRelationshipType());
                            } catch (URISyntaxException e) {
                              throw new RuntimeException(
                                  String.format(
                                      "Failed to convert urnStr %s found in the Graph to an Urn object",
                                      entity.getUrn()));
                            }
                          })
                      .collect(Collectors.toList()));

          return new EntityRelationships()
              .setStart(relatedEntitiesResult.getStart())
              .setCount(relatedEntitiesResult.getCount())
              .setTotal(relatedEntitiesResult.getTotal())
              .setRelationships(entityArray);
        },
        MetricRegistry.name(this.getClass(), "getLineage"));
  }

  @Nonnull
  @RestMethod.Delete
  public UpdateResponse delete(@QueryParam("urn") @Nonnull String rawUrn) throws Exception {
    Urn urn = Urn.createFromString(rawUrn);

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "deleteRelationships", urn.getEntityType()), _authorizer, auth, true);

    if (!isAPIAuthorizedUrns(
            opContext,
            LINEAGE, DELETE,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to delete entity: " + rawUrn);
    }
    _graphService.removeNode(systemOperationContext, urn);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  @Action(name = ACTION_GET_LINEAGE)
  @Nonnull
  @WithSpan
  public Task<EntityLineageResult> getLineage(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_DIRECTION) String direction,
      @ActionParam(PARAM_START) @Optional @Nullable Integer start,
      @ActionParam(PARAM_COUNT) @Optional @Nullable Integer count,
      @ActionParam(PARAM_MAX_HOPS) @Optional @Nullable Integer maxHops)
      throws URISyntaxException {
    log.info("GET LINEAGE {} {} {} {} {}", urnStr, direction, start, count, maxHops);
    final Urn urn = Urn.createFromString(urnStr);

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "getLineage", urn.getEntityType()), _authorizer, auth, true);

    if (!isAPIAuthorizedUrns(
            opContext,
            LINEAGE, READ,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity lineage: " + urnStr);
    }
    return RestliUtils.toTask(systemOperationContext,
        () ->
            _graphService.getLineage(systemOperationContext,
                urn,
                LineageDirection.valueOf(direction),
                start != null ? start : 0,
                count != null ? count : 100,
                maxHops != null ? maxHops : 1),
        MetricRegistry.name(this.getClass(), "getLineage"));
  }
}
