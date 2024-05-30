package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowseInput;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrowseResolver implements DataFetcher<CompletableFuture<BrowseResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private static final Logger _logger = LoggerFactory.getLogger(BrowseResolver.class.getName());

  private final Map<EntityType, BrowsableEntityType<?, ?>> _typeToEntity;

  public BrowseResolver(@Nonnull final List<BrowsableEntityType<?, ?>> browsableEntities) {
    _typeToEntity =
        browsableEntities.stream()
            .collect(Collectors.toMap(BrowsableEntityType::type, entity -> entity));
  }

  @Override
  public CompletableFuture<BrowseResults> get(DataFetchingEnvironment environment) {
    final BrowseInput input = bindArgument(environment.getArgument("input"), BrowseInput.class);

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _logger.debug(
                String.format(
                    "Executing browse. entity type: %s, path: %s, filters: %s, start: %s, count: %s",
                    input.getType(), input.getPath(), input.getFilters(), start, count));
            return _typeToEntity
                .get(input.getType())
                .browse(
                    input.getPath(), input.getFilters(), start, count, environment.getContext());
          } catch (Exception e) {
            _logger.error(
                "Failed to execute browse: "
                    + String.format(
                        "entity type: %s, path: %s, filters: %s, start: %s, count: %s",
                        input.getType(), input.getPath(), input.getFilters(), start, count)
                    + " "
                    + e.getMessage());
            throw new RuntimeException(
                "Failed to execute browse: "
                    + String.format(
                        "entity type: %s, path: %s, filters: %s, start: %s, count: %s",
                        input.getType(), input.getPath(), input.getFilters(), start, count),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
