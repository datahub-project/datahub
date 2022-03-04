package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageInput;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.GraphClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/**
 * GraphQL Resolver responsible for fetching lineage relationships between entities in the DataHub graph.
 * Lineage relationship denotes whether an entity is directly upstream or downstream of another entity
 */
public class EntityLineageResultResolver implements DataFetcher<CompletableFuture<EntityLineageResult>> {

  private final GraphClient _graphClient;

  public EntityLineageResultResolver(final GraphClient graphClient) {
    _graphClient = graphClient;
  }

  @Override
  public CompletableFuture<EntityLineageResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    final LineageInput input = bindArgument(environment.getArgument("input"), LineageInput.class);

    final LineageDirection lineageDirection = input.getDirection();
    @Nullable
    final Integer start = input.getStart(); // Optional!
    @Nullable
    final Integer count = input.getCount(); // Optional!

    com.linkedin.metadata.graph.LineageDirection resolvedDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(lineageDirection.toString());
    return CompletableFuture.supplyAsync(() -> mapEntityRelationships(lineageDirection,
        _graphClient.getLineageEntities(urn, resolvedDirection, start, count, 1, context.getActorUrn())));
  }

  private EntityLineageResult mapEntityRelationships(final LineageDirection lineageDirection,
      final com.linkedin.metadata.graph.EntityLineageResult entityLineageResult) {
    final EntityLineageResult result = new EntityLineageResult();
    result.setStart(entityLineageResult.getStart());
    result.setCount(entityLineageResult.getCount());
    result.setTotal(entityLineageResult.getTotal());
    result.setRelationships(entityLineageResult.getRelationships()
        .stream()
        .map(entityRelationship -> mapEntityRelationship(lineageDirection, entityRelationship))
        .collect(Collectors.toList()));
    return result;
  }

  private LineageRelationship mapEntityRelationship(final LineageDirection direction,
      final com.linkedin.metadata.graph.LineageRelationship lineageRelationship) {
    final LineageRelationship result = new LineageRelationship();
    final Entity partialEntity = UrnToEntityMapper.map(lineageRelationship.getEntity());
    if (partialEntity != null) {
      result.setEntity(partialEntity);
    }
    result.setType(lineageRelationship.getType());
    result.setDegree(lineageRelationship.getDegree());
    return result;
  }
}
