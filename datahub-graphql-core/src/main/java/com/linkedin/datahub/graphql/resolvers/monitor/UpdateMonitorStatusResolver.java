package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.UpdateMonitorStatusInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateMonitorStatusResolver implements DataFetcher<CompletableFuture<Monitor>> {

  private final MonitorService _monitorService;
  private final EntityClient _entityClient;

  public UpdateMonitorStatusResolver(
      @Nonnull final MonitorService monitorService, @Nonnull final EntityClient entityClient) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
  }

  @Override
  public CompletableFuture<Monitor> get(@Nonnull final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateMonitorStatusInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpdateMonitorStatusInput.class);
    final Urn monitorUrn = UrnUtils.getUrn(input.getUrn());
    final Urn entityUrn = UrnUtils.getUrn(monitorUrn.getEntityKey().get(0));

    return CompletableFuture.supplyAsync(
        () -> {
          if (isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {

            // Ensure that the entity exists.
            validateEntity(context.getOperationContext(), entityUrn);

            try {
              // Update provided monitor's state.
              _monitorService.upsertMonitorMode(
                  context.getOperationContext(),
                  monitorUrn,
                  MonitorMode.valueOf(input.getMode().toString()));
              return MonitorMapper.map(
                  context,
                  _monitorService.getMonitorEntityResponse(
                      context.getOperationContext(), monitorUrn));
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
    } catch (RemoteInvocationException e) {
      throw new DataHubGraphQLException(
          "An unknown error occurred!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }
}
