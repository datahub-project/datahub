package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityCountInput;
import com.linkedin.datahub.graphql.generated.EntityCountResult;
import com.linkedin.datahub.graphql.generated.EntityCountResults;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.opentelemetry.extension.annotations.WithSpan;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;

public class EntityCountsResolver implements DataFetcher<CompletableFuture<EntityCountResults>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  public EntityCountsResolver(final EntityClient entityClient, ViewService viewService) {
    _entityClient = entityClient;
    _viewService = viewService;
  }

  @Override
  @WithSpan
  public CompletableFuture<EntityCountResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    final EntityCountInput input =
        bindArgument(environment.getArgument("input"), EntityCountInput.class);
    final EntityCountResults results = new EntityCountResults();
    List<String> entityNames = getEntityNames(input.getTypes());

    return CompletableFuture.supplyAsync(
        () -> {
          final DataHubViewInfo maybeViewInfo = Objects.nonNull(input.getViewUrn()) ?
              resolveView(_viewService, UrnUtils.getUrn(input.getViewUrn()), context.getAuthentication()) : null;
          try {
            // First, get all counts
            Map<String, Long> gmsResult = _entityClient.batchGetTotalEntityCount(Objects.nonNull(maybeViewInfo) ?
                    SearchUtils.intersectEntityTypes(entityNames, maybeViewInfo.getDefinition().getEntityTypes())
                    : entityNames
                , context.getAuthentication());

            // bind to a result.
            List<EntityCountResult> resultList =
                gmsResult.entrySet().stream()
                    .map(
                        entry -> {
                          EntityCountResult result = new EntityCountResult();
                          result.setCount(Math.toIntExact(entry.getValue()));
                          result.setEntityType(EntityTypeMapper.getType(entry.getKey()));
                          return result;
                        })
                    .collect(Collectors.toList());
            results.setCounts(resultList);
            return results;
          } catch (Exception e) {
            throw new RuntimeException("Failed to get entity counts", e);
          }
        });
  }
}
