package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.UpdateAssertionMonitorSettingsInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionMonitorSettings;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateAssertionMonitorSettingsResolver
    implements DataFetcher<CompletableFuture<Monitor>> {

  private final MonitorService _monitorService;
  private final EntityClient _entityClient;

  public UpdateAssertionMonitorSettingsResolver(
      @Nonnull final MonitorService monitorService, @Nonnull final EntityClient entityClient) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
  }

  @Override
  public CompletableFuture<Monitor> get(@Nonnull final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateAssertionMonitorSettingsInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpdateAssertionMonitorSettingsInput.class);
    final Urn monitorUrn = UrnUtils.getUrn(input.getUrn());
    final Urn entityUrn = UrnUtils.getUrn(monitorUrn.getEntityKey().get(0));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {

            // Ensure that the entity exists.
            validateEntity(context.getOperationContext(), entityUrn);

            try {
              // Get existing monitor info
              MonitorInfo monitorInfo =
                  _monitorService.getMonitorInfo(context.getOperationContext(), monitorUrn);

              if (monitorInfo == null || monitorInfo.getAssertionMonitor() == null) {
                throw new DataHubGraphQLException(
                    String.format("Monitor with urn %s is not an assertion monitor", monitorUrn),
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }

              // Update settings
              AssertionMonitorSettings settings = monitorInfo.getAssertionMonitor().getSettings();
              if (settings == null) {
                // Create if it does not exist
                settings = new AssertionMonitorSettings();
                monitorInfo.getAssertionMonitor().setSettings(settings);
              }

              settings.setAdjustmentSettings(
                  AssertionMapper.mapGraphQLAssertionAdjustmentSettings(
                      input.getAdjustmentSettings()));

              // Update the monitor
              this._entityClient.ingestProposal(
                  context.getOperationContext(),
                  AspectUtils.buildMetadataChangeProposal(
                      monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, monitorInfo),
                  false);

              // Return updated monitor
              return MonitorMapper.map(
                  context,
                  _monitorService.getMonitorEntityResponse(
                      context.getOperationContext(), monitorUrn));
            } catch (Exception e) {
              log.error("Failed to update Assertion Monitor Settings!", e);
              throw new DataHubGraphQLException(
                  "Failed to update Assertion Monitor Settings! An unknown error occurred.",
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Verifies that an entity exists, else throws a {@link DataHubGraphQLException}. */
  private void validateEntity(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      if (!this._entityClient.exists(opContext, entityUrn)) {
        throw new DataHubGraphQLException(
            String.format(
                "Failed to edit Monitor. %s with urn %s does not exist.",
                entityUrn.getEntityType(), entityUrn),
            DataHubGraphQLErrorCode.NOT_FOUND);
      }
    } catch (RemoteInvocationException e) {
      throw new DataHubGraphQLException(
          "An unknown error occurred!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }
}
