package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Relationship;
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
import java.util.stream.Stream;


/**
 * GraphQL Resolver responsible for fetching relationships between entities in the DataHub graph.
 */
public class RelationshipResolver implements DataFetcher<CompletableFuture<List<Relationship>>> {

  private final RelationshipClient _client;

  public RelationshipResolver(final RelationshipClient client) {
    _client = client;
  }

  @Override
  public CompletableFuture<List<Relationship>> get(DataFetchingEnvironment environment) {

      final String urn = ((Entity) environment.getSource()).getUrn();
      final String relationshipType = environment.getArgument("type");
      final String relationshipDirection = environment.getArgument("direction");

      if (relationshipDirection != null) {

        return CompletableFuture.supplyAsync(() -> fetchEntityRelationships(
            urn,
            relationshipType,
            RelationshipDirection.valueOf(relationshipDirection)
        ).getEntities().stream().map(entityRelationship -> mapEntityRelationship(
            relationshipType,
            com.linkedin.datahub.graphql.generated.RelationshipDirection.valueOf(relationshipDirection),
            entityRelationship)
        ).collect(Collectors.toList()));
      } else {

        final CompletableFuture<List<Relationship>> incomingRelationshipFutures = CompletableFuture.supplyAsync(() ->
          fetchEntityRelationships(urn, relationshipType, RelationshipDirection.INCOMING)
            .getEntities()
            .stream()
            .map(entityRelationship -> mapEntityRelationship(
                relationshipType,
                com.linkedin.datahub.graphql.generated.RelationshipDirection.INCOMING,
                entityRelationship)
            ).collect(Collectors.toList()));

        final CompletableFuture<List<Relationship>> outgoingRelationshipFutures = CompletableFuture.supplyAsync(() ->
          fetchEntityRelationships(urn, relationshipType, RelationshipDirection.OUTGOING)
            .getEntities()
            .stream()
            .map(entityRelationship -> mapEntityRelationship(relationshipType, com.linkedin.datahub.graphql.generated.RelationshipDirection.OUTGOING,
                entityRelationship))
            .collect(Collectors.toList()));

        return CompletableFuture.allOf(incomingRelationshipFutures, outgoingRelationshipFutures).thenApplyAsync((it) -> {
            List<Relationship> incomingRelationships = incomingRelationshipFutures.join();
            List<Relationship> outgoingRelationships = outgoingRelationshipFutures.join();
            return Stream.concat(incomingRelationships.stream(), outgoingRelationships.stream()).collect(Collectors.toList());
        });
      }
  }

  private EntityRelationships fetchEntityRelationships(final String urn,  final String type, final RelationshipDirection direction) {
    try {
      return _client.getRelationships(urn, direction, ImmutableList.of(type));
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException("Failed to retrieve aspects from GMS", e);
    }
  }

  private Relationship mapEntityRelationship(
      final String type,
      final com.linkedin.datahub.graphql.generated.RelationshipDirection direction,
      final EntityRelationship entityRelationship) {
    final Relationship result = new Relationship();
    final Entity partialEntity = UrnToEntityMapper.map(entityRelationship.getEntity());
    if (partialEntity != null) {
      result.setEntity(partialEntity);
    }
    result.setType(type);
    result.setDirection(direction);
    return result;
  }
}