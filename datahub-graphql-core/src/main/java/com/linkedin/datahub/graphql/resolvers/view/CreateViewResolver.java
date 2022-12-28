package com.linkedin.datahub.graphql.resolvers.view;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateViewInput;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.DataHubViewDefinition;
import com.linkedin.datahub.graphql.generated.DataHubViewFilter;
import com.linkedin.datahub.graphql.generated.FacetFilter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolver responsible for updating a particular DataHub View
 */
@Slf4j
public class CreateViewResolver implements DataFetcher<CompletableFuture<DataHubView>> {

  private final ViewService _viewService;

  public CreateViewResolver(@Nonnull final ViewService viewService) {
    _viewService = Objects.requireNonNull(viewService);
  }

  @Override
  public CompletableFuture<DataHubView> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateViewInput input = bindArgument(environment.getArgument("input"), CreateViewInput.class);

    return CompletableFuture.supplyAsync(() -> {
      if (ViewUtils.canCreateView(
          DataHubViewType.valueOf(input.getViewType().toString()),
          context)) {
        try {
          final Urn urn = _viewService.createView(
              DataHubViewType.valueOf(input.getViewType().toString()),
              input.getName(),
              input.getDescription(),
              ViewUtils.mapDefinition(input.getDefinition()),
              context.getAuthentication(),
              System.currentTimeMillis());
          return createView(urn, input);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create View with input: %s", input), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private DataHubView createView(@Nonnull final Urn urn, @Nonnull final CreateViewInput input) {
    return new DataHubView(
        urn.toString(),
        com.linkedin.datahub.graphql.generated.EntityType.DATAHUB_VIEW,
        input.getViewType(),
        input.getName(),
        input.getDescription(),
        new DataHubViewDefinition(
            input.getDefinition().getEntityTypes(),
            new DataHubViewFilter(
                input.getDefinition().getFilter().getOperator(),
                input.getDefinition().getFilter().getFilters().stream().map(filterInput ->
                    new FacetFilter(filterInput.getField(), filterInput.getCondition(), filterInput.getValues(),
                        filterInput.getNegated()))
                    .collect(Collectors.toList()))));
  }
}
