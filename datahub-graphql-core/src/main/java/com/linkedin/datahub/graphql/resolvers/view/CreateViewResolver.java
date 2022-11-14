package com.linkedin.datahub.graphql.resolvers.view;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateViewInput;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewType;
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
public class CreateViewResolver implements DataFetcher<CompletableFuture<String>> {

  private final ViewService _viewService;

  public CreateViewResolver(@Nonnull final ViewService viewService) {
    _viewService = Objects.requireNonNull(viewService);
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateViewInput input = bindArgument(environment.getArgument("input"), CreateViewInput.class);

    return CompletableFuture.supplyAsync(() -> {
      if (ViewUtils.canCreateView(
          DataHubViewType.valueOf(input.getViewType().toString()),
          context)) {
        try {
          return _viewService.createView(
              DataHubViewType.valueOf(input.getViewType().toString()),
              input.getName(),
              input.getDescription(),
              ViewUtils.mapDefinition(input.getDefinition()),
              context.getAuthentication(),
              System.currentTimeMillis()).toString();
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create View with input: %s", input), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}
