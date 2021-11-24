package com.linkedin.datahub.graphql.resolvers.ingest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceRuns;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IngestionSourceRunsResolver implements DataFetcher<CompletableFuture<IngestionSourceRuns>> {

  private final GraphClient _graphClient;
  private final EntityClient _entityClient;

  public IngestionSourceRunsResolver(
      final GraphClient graphClient,
      final EntityClient entityClient
      ) {
    _graphClient = graphClient;
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<IngestionSourceRuns> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((IngestionSource) environment.getSource()).getUrn();
    final Integer start = environment.getArgument("start") != null ? environment.getArgument("start") : 0;
    final Integer count = environment.getArgument("count") != null ? environment.getArgument("count") : 10;

    return CompletableFuture.supplyAsync(() -> {

      try {

        // 1. Fetch the related edges
        final EntityRelationships relationships = _graphClient.getRelatedEntities(
            urn,
            ImmutableList.of("ingestionSource"),
            RelationshipDirection.INCOMING,
            start,
            count,
            context.getActorUrn()
        );

        // 2. Batch fetch the related ExecutionRequests
        final Set<Urn> relatedExecRequests = relationships.getRelationships().stream()
            .map(EntityRelationship::getEntity)
            .collect(Collectors.toSet());

        final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            relatedExecRequests,
            ImmutableSet.of(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME),
            context.getAuthentication());

        // 3. Map the GMS ExecutionRequests into GraphQL Execution Requests
        final IngestionSourceRuns result = new IngestionSourceRuns();
        result.setStart(relationships.getStart());
        result.setCount(relationships.getCount());
        result.setTotal(relationships.getTotal());
        result.setExecutionRequests(IngestionResolverUtils.mapExecutionRequests(entities.values()));
        return result;
      } catch (Exception e) {
        throw new DataHubGraphQLException(
            String.format("Failed to resolve runs associated with ingestion with urn %s", urn), DataHubGraphQLErrorCode.SERVER_ERROR);
      }
    });
  }
}
