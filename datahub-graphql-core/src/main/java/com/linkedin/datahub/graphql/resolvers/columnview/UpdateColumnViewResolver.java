package com.linkedin.datahub.graphql.resolvers.columnview;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataHubColumnView;
import com.linkedin.datahub.graphql.generated.UpdateColumnViewInput;
import com.linkedin.datahub.graphql.types.columnview.DataHubColumnViewMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.ColumnViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for updating a particular DataHub Column View. */
@Slf4j
public class UpdateColumnViewResolver implements DataFetcher<CompletableFuture<DataHubColumnView>> {

  private final ColumnViewService _columnViewService;

  public UpdateColumnViewResolver(@Nonnull final ColumnViewService columnViewService) {
    _columnViewService =
        Objects.requireNonNull(columnViewService, "columnViewService must not be null");
  }

  @Override
  public CompletableFuture<DataHubColumnView> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final UpdateColumnViewInput input =
        bindArgument(environment.getArgument("input"), UpdateColumnViewInput.class);

    final Urn urn = Urn.createFromString(urnStr);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (ColumnViewUtils.canUpdateColumnView(_columnViewService, urn, context)) {
              _columnViewService.updateColumnView(
                  context.getOperationContext(),
                  urn,
                  input.getName(),
                  input.getDescription(),
                  input.getDefinition() == null
                      ? null
                      : ColumnViewUtils.mapDefinition(
                          input.getDefinition(),
                          context.getOperationContext().getAspectRetriever()),
                  System.currentTimeMillis());
              log.info(String.format("Successfully updated Column View %s with urn", urn));
              final EntityResponse response =
                  _columnViewService.getColumnViewEntityResponse(
                      context.getOperationContext(), urn);
              if (response == null) {
                throw new RuntimeException(
                    String.format(
                        "Failed to perform update to Column View with urn %s. Failed to find it in GMS.",
                        urn));
              }
              return DataHubColumnViewMapper.map(context, response);
            }
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against Column View with urn %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
