package com.linkedin.datahub.graphql.resolvers.action.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.action.ActionPipelineType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ListActionPipelineResolver
    implements DataFetcher<CompletableFuture<ListActionPipelinesResult>> {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListActionPipelinesResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageActionPipelines(context)) {

      final ListActionPipelinesInput input =
          bindArgument(environment.getArgument("input"), ListActionPipelinesInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
      final List<FacetFilterInput> filters =
          input.getFilters() == null ? Collections.emptyList() : input.getFilters();

      log.info("ListActionPipelineResolver called with input: {}", input);

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // First, get all actions pipeline urns
              final SearchResult gmsResult =
                  _entityClient.search(
                      context
                          .getOperationContext()
                          .withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
                      Constants.ACTIONS_PIPELINE_ENTITY_NAME,
                      query,
                      buildFilter(filters, Collections.emptyList()),
                      null,
                      start,
                      count);

              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      Constants.ACTIONS_PIPELINE_ENTITY_NAME,
                      new HashSet<>(
                          gmsResult.getEntities().stream()
                              .map(SearchEntity::getEntity)
                              .collect(Collectors.toList())),
                      ActionPipelineType.ASPECTS_TO_FETCH);

              // Now that we have entities we can bind this to a result.
              final ListActionPipelinesResult result = new ListActionPipelinesResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setActionPipelines(
                  entities.values().stream()
                      .map(ActionPipelineType::map)
                      .collect(Collectors.toList()));
              return result;

            } catch (Exception e) {
              throw new RuntimeException("Failed to list actions pipelines", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
