package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityCountInput;
import com.linkedin.datahub.graphql.generated.EntityCountResult;
import com.linkedin.datahub.graphql.generated.EntityCountResults;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class EntityCountsResolver implements DataFetcher<CompletableFuture<EntityCountResults>> {

  private final EntityClient _entityClient;

  public EntityCountsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  @WithSpan
  public CompletableFuture<EntityCountResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    final EntityCountInput input =
        bindArgument(environment.getArgument("input"), EntityCountInput.class);
    final EntityCountResults results = new EntityCountResults();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // First, get all counts
            Map<String, Long> gmsResult =
                _entityClient.batchGetTotalEntityCount(
                    input.getTypes().stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList()),
                    context.getAuthentication());

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
