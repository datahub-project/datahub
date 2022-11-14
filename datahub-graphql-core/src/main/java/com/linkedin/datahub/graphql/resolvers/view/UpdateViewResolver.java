package com.linkedin.datahub.graphql.resolvers.view;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateViewInput;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver responsible for updating a particular DataHub View
 */
@Slf4j
public class UpdateViewResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ViewService _viewService;

  public UpdateViewResolver(@Nonnull final ViewService viewService) {
    _viewService = Objects.requireNonNull(viewService, "viewService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final UpdateViewInput input = bindArgument(environment.getArgument("input"), UpdateViewInput.class);

    final Urn urn = Urn.createFromString(urnStr);
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (ViewUtils.canUpdateView(_viewService, urn, context)) {
            _viewService.updateView(
                urn,
                input.getName(),
                input.getDescription(),
                ViewUtils.mapDefinition(input.getDefinition()),
                context.getAuthentication(),
                System.currentTimeMillis());
            log.info(String.format("Successfully updated View %s with urn", urn));
            return true;
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      } catch (AuthorizationException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against View with urn %s", urn), e);
      }
    });
  }
}
