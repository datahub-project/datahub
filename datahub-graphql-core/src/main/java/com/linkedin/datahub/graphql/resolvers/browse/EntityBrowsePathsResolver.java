package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class EntityBrowsePathsResolver implements DataFetcher<CompletableFuture<List<BrowsePath>>> {

  private final BrowsableEntityType<?, ?> _browsableType;

  public EntityBrowsePathsResolver(@Nonnull final BrowsableEntityType<?, ?> browsableType) {
    _browsableType = browsableType;
  }

  @Override
  public CompletableFuture<List<BrowsePath>> get(DataFetchingEnvironment environment) {

    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();
    if (!canView(context.getOperationContext(), UrnUtils.getUrn(urn))) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _browsableType.browsePaths(urn, context);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to retrieve browse paths for entity with urn %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
