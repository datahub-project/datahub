package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.RelationshipsInput;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 */
public class EntityRelationshipsResultResolver
    implements DataFetcher<CompletableFuture<EntityRelationshipsResult>> {

  private final GraphClient _graphClient;

  public EntityRelationshipsResultResolver(final GraphClient graphClient) {
    _graphClient = graphClient;
  }

  @Override
  public CompletableFuture<EntityRelationshipsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    final RelationshipsInput input =
        bindArgument(environment.getArgument("input"), RelationshipsInput.class);

    final List<String> relationshipTypes = input.getTypes();
    final com.linkedin.datahub.graphql.generated.RelationshipDirection relationshipDirection =
        input.getDirection();
    final Integer start = input.getStart(); // Optional!
    final Integer count = input.getCount(); // Optional!
    final RelationshipDirection resolvedDirection =
        RelationshipDirection.valueOf(relationshipDirection.toString());
    return CompletableFuture.supplyAsync(
        () ->
            mapEntityRelationships(
                fetchEntityRelationships(
                    urn, relationshipTypes, resolvedDirection, start, count, context.getActorUrn()),
                resolvedDirection));
  }

  private EntityRelationships fetchEntityRelationships(
      final String urn,
      final List<String> types,
      final RelationshipDirection direction,
      final Integer start,
      final Integer count,
      final String actor) {

    return _graphClient.getRelatedEntities(urn, types, direction, start, count, actor);
  }

  private EntityRelationshipsResult mapEntityRelationships(
      final EntityRelationships entityRelationships,
      final RelationshipDirection relationshipDirection) {
    final EntityRelationshipsResult result = new EntityRelationshipsResult();
    result.setStart(entityRelationships.getStart());
    result.setCount(entityRelationships.getCount());
    result.setTotal(entityRelationships.getTotal());
    result.setRelationships(
        entityRelationships.getRelationships().stream()
            .map(
                entityRelationship ->
                    mapEntityRelationship(
                        com.linkedin.datahub.graphql.generated.RelationshipDirection.valueOf(
                            relationshipDirection.name()),
                        entityRelationship))
            .collect(Collectors.toList()));
    return result;
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship mapEntityRelationship(
      final com.linkedin.datahub.graphql.generated.RelationshipDirection direction,
      final EntityRelationship entityRelationship) {
    final com.linkedin.datahub.graphql.generated.EntityRelationship result =
        new com.linkedin.datahub.graphql.generated.EntityRelationship();
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
