package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateAssertionMonitorInput;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;


@Slf4j
public class CreateAssertionMonitorResolver implements DataFetcher<CompletableFuture<Monitor>> {

  private final MonitorService _monitorService;

  public CreateAssertionMonitorResolver(@Nonnull final MonitorService monitorService) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
  }

  @Override
  public CompletableFuture<Monitor> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateAssertionMonitorInput
        input = ResolverUtils.bindArgument(environment.getArgument("input"), CreateAssertionMonitorInput.class);
    final Urn assertionUrn = UrnUtils.getUrn(input.getAssertionUrn());
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {

        try {
          // First create the new monitor
          final Urn monitorUrn = _monitorService.createAssertionMonitor(
              entityUrn,
              assertionUrn,
              createCronSchedule(input.getSchedule()),
              createAssertionEvaluationParameters(input.getParameters()),
              input.getExecutorId(),
              context.getAuthentication()
          );

          // Then, return the new monitor
          return MonitorMapper.map(_monitorService.getMonitorEntityResponse(monitorUrn, context.getAuthentication()));
        } catch (Exception e) {
          log.error("Failed to create Assertion monitor!", e);
          throw new DataHubGraphQLException("Failed to create Assertion Monitor! An unknown error occurred.", DataHubGraphQLErrorCode.SERVER_ERROR);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}