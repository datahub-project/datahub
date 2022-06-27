package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityLineageResult;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.LineageInput;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.SiblingGraphService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/**
 * GraphQL Resolver responsible for fetching lineage relationships between entities in the DataHub graph.
 * Lineage relationship denotes whether an entity is directly upstream or downstream of another entity
 */
@Slf4j
public class EntityLineageResultResolver implements DataFetcher<CompletableFuture<EntityLineageResult>> {

  private final SiblingGraphService _siblingGraphService;

  public EntityLineageResultResolver(final SiblingGraphService siblingGraphService) {
    _siblingGraphService = siblingGraphService;
  }

  @Override
  public CompletableFuture<EntityLineageResult> get(DataFetchingEnvironment environment) {
    final String urn = ((Entity) environment.getSource()).getUrn();
    final LineageInput input = bindArgument(environment.getArgument("input"), LineageInput.class);

    final LineageDirection lineageDirection = input.getDirection();
    @Nullable
    final Integer start = input.getStart(); // Optional!
    @Nullable
    final Integer count = input.getCount(); // Optional!

    com.linkedin.metadata.graph.LineageDirection resolvedDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(lineageDirection.toString());

    return CompletableFuture.supplyAsync(() -> {
      try {
        return mapEntityRelationships(lineageDirection,
            _siblingGraphService.getLineage(Urn.createFromString(urn), resolvedDirection, start != null ? start : 0, count != null ? count : 100, 1));
      } catch (URISyntaxException e) {
        log.error("Failed to fetch lineage for {}", urn);
        throw new RuntimeException(String.format("Failed to fetch lineage for {}", urn), e);
      }
    });
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
