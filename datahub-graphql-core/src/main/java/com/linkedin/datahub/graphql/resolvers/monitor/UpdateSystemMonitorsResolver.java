package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateSystemMonitorInput;
import com.linkedin.datahub.graphql.generated.UpdateSystemMonitorsInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorMode;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateSystemMonitorsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final MonitorService _monitorService;
  private final EntityClient _entityClient;

  public UpdateSystemMonitorsResolver(
      @Nonnull final MonitorService monitorService, @Nonnull final EntityClient entityClient) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
  }

  @Override
  public CompletableFuture<Boolean> get(@Nonnull final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateSystemMonitorsInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpdateSystemMonitorsInput.class);
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {

            // Ensure that the entity exists.
            validateEntity(context.getOperationContext(), entityUrn);

            try {
              for (final UpdateSystemMonitorInput monitorInput : input.getMonitors()) {
                // 1. Convert the system monitor type into a unique urn.
                final Urn monitorUrn =
                    getMonitorUrnForSystemMonitorType(entityUrn, monitorInput.getType());

                // 2. Update each provided monitor's state.
                _monitorService.upsertMonitorMode(
                    context.getOperationContext(),
                    monitorUrn,
                    MonitorMode.valueOf(monitorInput.getMode().toString()));
              }

              return true;
            } catch (Exception e) {
              log.error("Failed to update System Monitors!", e);
              throw new DataHubGraphQLException(
                  "Failed to update System Monitors! An unknown error occurred.",
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  /** Verifies that an entity exists, else throws a {@link DataHubGraphQLException}. */
  private void validateEntity(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      if (!this._entityClient.exists(opContext, entityUrn)) {
        throw new DataHubGraphQLException(
            String.format(
                "Failed to edit Monitor. %s with urn %s does not exist.",
                entityUrn.getEntityType(), entityUrn),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    } catch (Exception e) {
      throw new DataHubGraphQLException(
          "An unknown error occurred!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }
}
