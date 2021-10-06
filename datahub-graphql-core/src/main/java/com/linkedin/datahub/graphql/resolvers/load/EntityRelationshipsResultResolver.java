package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.RelationshipsInput;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.lineage.client.RelationshipClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.*;


/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 */
public class EntityRelationshipsResultResolver implements DataFetcher<CompletableFuture<EntityRelationshipsResult>> {

  private static final Integer MAX_DOWNSTREAM_CNT = 100;
  private final GraphService _graphService;

  public EntityRelationshipsResultResolver(final GraphService graphService) {
    _graphService = graphService;
  }

  private EntityRelationships getRelatedEntities(
      String rawUrn,
      List<String> relationshipTypes,
      RelationshipDirection direction,
      @Nullable Integer start,
      @Nullable Integer count) {

    start = start == null ? 0 : start;
    count = count == null ? MAX_DOWNSTREAM_CNT : count;

    RelatedEntitiesResult relatedEntitiesResult =
        _graphService.findRelatedEntities("", newFilter("urn", rawUrn), "", EMPTY_FILTER, relationshipTypes,
            newRelationshipFilter(EMPTY_FILTER, direction), start, count);

    final EntityRelationshipArray entityArray = new EntityRelationshipArray(
        relatedEntitiesResult.getEntities().stream().map(
            entity -> {
              try {
                return new EntityRelationship()
                    .setEntity(Urn.createFromString(entity.getUrn()))
                    .setType(entity.getRelationshipType());
              } catch (URISyntaxException e) {
                throw new RuntimeException(
                    String.format("Failed to convert urnStr %s found in the Graph to an Urn object", entity.getUrn()));
              }
            }
        ).collect(Collectors.toList())
    );

    return new EntityRelationships()
        .setStart(relatedEntitiesResult.getStart())
        .setCount(relatedEntitiesResult.getCount())
        .setTotal(relatedEntitiesResult.getTotal())
        .setRelationships(entityArray);
  }

  @Override
  public CompletableFuture<EntityRelationshipsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
      final RelationshipsInput input = bindArgument(environment.getArgument("input"), RelationshipsInput.class);

      final List<String> relationshipTypes = input.getTypes();
      final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection = input.getDirection();
      final Integer start = input.getStart(); // Optional!
      final Integer count = input.getCount(); // Optional!
      final RelationshipDirection resolvedDirection = RelationshipDirection.valueOf(relationshipDirection.toString());
      return CompletableFuture.supplyAsync(() -> mapEntityRelationships(
            fetchEntityRelationships(
              urn,
              relationshipTypes,
              resolvedDirection,
              start,
              count,
              context.getActor()
            ),
          resolvedDirection
      ));
  }

  private EntityRelationships fetchEntityRelationships(
      final String urn,
      final List<String> types,
      final RelationshipDirection direction,
      final Integer start,
      final Integer count,
      final String actor) {

      return getRelatedEntities(urn, types, direction, start, count);
  }

  private EntityRelationshipsResult mapEntityRelationships(
      final EntityRelationships entityRelationships,
      final RelationshipDirection relationshipDirection
  ) {
    final EntityRelationshipsResult result = new EntityRelationshipsResult();
    result.setStart(entityRelationships.getStart());
    result.setCount(entityRelationships.getCount());
    result.setTotal(entityRelationships.getTotal());
    result.setRelationships(entityRelationships.getRelationships().stream().map(entityRelationship -> mapEntityRelationship(
        com.linkedin.datahub.graphql.generated.RelationshipDirection.valueOf(relationshipDirection.name()),
        entityRelationship)
    ).collect(Collectors.toList()));
    return result;
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship mapEntityRelationship(
      final com.linkedin.datahub.graphql.generated.RelationshipDirection direction,
      final EntityRelationship entityRelationship) {
    final com.linkedin.datahub.graphql.generated.EntityRelationship result = new com.linkedin.datahub.graphql.generated.EntityRelationship();
    final Entity partialEntity = UrnToEntityMapper.map(entityRelationship.getEntity());
    if (partialEntity != null) {
      result.setEntity(partialEntity);
    }
    result.setType(entityRelationship.getType());
    result.setDirection(direction);
    if (entityRelationship.hasCreated()) {
      result.setCreated(AuditStampMapper.map(entityRelationship.getCreated()));
    }
    return result;
  }
}
