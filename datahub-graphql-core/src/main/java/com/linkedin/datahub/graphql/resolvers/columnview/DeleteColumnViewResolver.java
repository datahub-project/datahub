package com.linkedin.datahub.graphql.resolvers.columnview;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.ColumnViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for hard deleting a particular DataHub Column View. */
@Slf4j
public class DeleteColumnViewResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ColumnViewService _columnViewService;

  public DeleteColumnViewResolver(@Nonnull final ColumnViewService columnViewService) {
    _columnViewService =
        Objects.requireNonNull(columnViewService, "columnViewService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(urnStr);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (ColumnViewUtils.canUpdateColumnView(_columnViewService, urn, context)) {
              _columnViewService.deleteColumnView(context.getOperationContext(), urn);
              log.info(String.format("Successfully deleted Column View %s with urn", urn));
              return true;
            }
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform delete against Column View with urn %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
