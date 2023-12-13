package com.linkedin.datahub.graphql.resolvers.view;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for hard deleting a particular DataHub View */
@Slf4j
public class DeleteViewResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ViewService _viewService;

  public DeleteViewResolver(@Nonnull final ViewService viewService) {
    _viewService = Objects.requireNonNull(viewService, "viewService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(urnStr);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (ViewUtils.canUpdateView(_viewService, urn, context)) {
              _viewService.deleteView(urn, context.getAuthentication());
              log.info(String.format("Successfully deleted View %s with urn", urn));
              return true;
            }
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform delete against View with urn %s", urn), e);
          }
        });
  }
}
