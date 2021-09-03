package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.lineage.client.RelationshipClient;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 */
public class EntityRelationshipsResultResolver implements DataFetcher<CompletableFuture<EntityRelationshipsResult>> {

  private final RelationshipClient _client;

  public EntityRelationshipsResultResolver(final RelationshipClient client) {
    _client = client;
  }

  @Override
  public CompletableFuture<EntityRelationshipsResult> get(DataFetchingEnvironment environment) {
      final QueryContext context = (QueryContext) environment.getContext();
      final String urn = ((Entity) environment.getSource()).getUrn();
      final List<String> relationshipTypes = environment.getArgument("types");
      final String relationshipDirection = environment.getArgument("direction");
      final Integer start = environment.getArgument("start"); // Optional!
      final Integer count = environment.getArgument("count"); // Optional!
      final RelationshipDirection resolvedDirection = RelationshipDirection.valueOf(relationshipDirection);
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
    try {
      return _client.getRelationships(urn, direction, types, start, count, actor);
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException("Failed to retrieve aspects from GMS", e);
    }
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