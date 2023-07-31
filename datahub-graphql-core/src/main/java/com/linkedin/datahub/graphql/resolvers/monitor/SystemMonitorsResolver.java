package com.linkedin.datahub.graphql.resolvers.monitor;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.SystemMonitor;
import com.linkedin.datahub.graphql.generated.SystemMonitorType;
import com.linkedin.datahub.graphql.generated.SystemMonitorsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;


@Slf4j
public class SystemMonitorsResolver implements DataFetcher<CompletableFuture<SystemMonitorsResult>> {

  private final MonitorService _monitorService;
  private final EntityClient _entityClient;

  public SystemMonitorsResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final EntityClient entityClient) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
  }

  @Override
  public CompletableFuture<SystemMonitorsResult> get(@Nonnull final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn entityUrn = UrnUtils.getUrn(environment.getArgument("urn"));

    return CompletableFuture.supplyAsync(() -> {
      // Ensure that the entity exists.
      validateEntity(entityUrn, context.getAuthentication());

      // Next, fetch the system monitors that are active for the entity.
      // To do this, we issue direct lookups.
      final SystemMonitorsResult result = new SystemMonitorsResult();
      final List<SystemMonitor> monitors = getSystemMonitorsForEntity(entityUrn, context.getAuthentication());
      result.setMonitors(monitors);
      return result;
    });
  }

  /**
   * Verifies that an entity exists and has system monitors support, else throws a {@link DataHubGraphQLException}.
   */
  private void validateEntity(@Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) {
    if (!ENTITY_TYPES_WITH_SYSTEM_MONITORS.contains(entityUrn.getEntityType())) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to fetch system monitors for entity with urn %s. System monitors are not supported for "
              + "this entity type", entityUrn),
          DataHubGraphQLErrorCode.BAD_REQUEST
      );
    }
    try {
      if (!this._entityClient.exists(entityUrn, authentication)) {
        throw new DataHubGraphQLException(
            String.format("Failed to edit Monitor. %s with urn %s does not exist.", entityUrn.getEntityType(), entityUrn),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    } catch (RemoteInvocationException e) {
      throw new DataHubGraphQLException(
          "An unknown error occurred!",
          DataHubGraphQLErrorCode.SERVER_ERROR
      );
    }
  }

  /**
   * Returns monitors that already exist for the entity. If a monitor does not exist, it is not active.
   */
  private List<SystemMonitor> getSystemMonitorsForEntity(@Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) {
    final List<SystemMonitor> monitors = new ArrayList<>();

    // 1. Try to add SLA Monitor Type.
    Urn slaMonitorUrn = getMonitorUrnForSystemMonitorType(entityUrn, SystemMonitorType.FRESHNESS);
    if (_monitorService.getMonitorInfo(slaMonitorUrn) != null) {
      Monitor partialMonitor = new Monitor();
      partialMonitor.setUrn(slaMonitorUrn.toString());
      monitors.add(new SystemMonitor(SystemMonitorType.FRESHNESS, partialMonitor));
    }

    return monitors;
  }
}
