package com.linkedin.datahub.graphql.resolvers.columnview;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateColumnViewInput;
import com.linkedin.datahub.graphql.generated.DataHubColumnView;
import com.linkedin.datahub.graphql.types.columnview.DataHubColumnViewMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.ColumnViewService;
import com.linkedin.view.DataHubColumnViewTarget;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for creating a DataHub Column View. */
@Slf4j
public class CreateColumnViewResolver implements DataFetcher<CompletableFuture<DataHubColumnView>> {

  private final ColumnViewService _columnViewService;

  public CreateColumnViewResolver(@Nonnull final ColumnViewService columnViewService) {
    _columnViewService = Objects.requireNonNull(columnViewService);
  }

  @Override
  public CompletableFuture<DataHubColumnView> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateColumnViewInput input =
        bindArgument(environment.getArgument("input"), CreateColumnViewInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final DataHubViewType viewType = DataHubViewType.valueOf(input.getViewType().toString());
          if (ColumnViewUtils.canCreateColumnView(viewType, context)) {
            try {
              final DataHubColumnViewTarget target =
                  input.getTarget() == null
                      ? DataHubColumnViewTarget.DATASET_SCHEMA_FIELDS
                      : DataHubColumnViewTarget.valueOf(input.getTarget().toString());
              final Urn urn =
                  _columnViewService.createColumnView(
                      context.getOperationContext(),
                      viewType,
                      target,
                      input.getName(),
                      input.getDescription(),
                      ColumnViewUtils.mapDefinition(
                          input.getDefinition(),
                          context.getOperationContext().getAspectRetriever()),
                      System.currentTimeMillis());
              // Re-read rather than hand-build: the definition echo is non-trivial (nested
              // column stubs) and the Views precedent of rebuilding by hand is brittle.
              final EntityResponse response =
                  _columnViewService.getColumnViewEntityResponse(
                      context.getOperationContext(), urn);
              if (response == null) {
                throw new RuntimeException(
                    String.format("Created Column View %s but failed to read it back", urn));
              }
              return DataHubColumnViewMapper.map(context, response);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to create Column View with input: %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
