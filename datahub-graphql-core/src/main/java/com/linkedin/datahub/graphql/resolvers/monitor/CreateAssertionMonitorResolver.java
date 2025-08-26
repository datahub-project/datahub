package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateAssertionMonitorInput;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateAssertionMonitorResolver implements DataFetcher<CompletableFuture<Monitor>> {

  private final MonitorService _monitorService;
  private final AssertionService _assertionService;

  public CreateAssertionMonitorResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final AssertionService assertionService) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<Monitor> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateAssertionMonitorInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), CreateAssertionMonitorInput.class);
    final Urn assertionUrn = UrnUtils.getUrn(input.getAssertionUrn());
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (isAuthorizedToCreateMonitor(entityUrn, assertionUrn, context)) {
            try {
              // First create the new monitor
              final Urn monitorUrn =
                  _monitorService.createAssertionMonitor(
                      context.getOperationContext(),
                      entityUrn,
                      assertionUrn,
                      createCronSchedule(input.getSchedule()),
                      createAssertionEvaluationParameters(input.getParameters()),
                      input.getExecutorId());

              // Then, return the new monitor
              return MonitorMapper.map(
                  context,
                  _monitorService.getMonitorEntityResponse(
                      context.getOperationContext(), monitorUrn));
            } catch (Exception e) {
              log.error("Failed to create Assertion monitor!", e);
              throw new DataHubGraphQLException(
                  "Failed to create Assertion Monitor! An unknown error occurred.",
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean isAuthorizedToCreateMonitor(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final QueryContext context) {
    // If the monitor requires heightened checks (SQL monitors), then we require them here.
    AssertionInfo assertion =
        _assertionService.getAssertionInfo(context.getOperationContext(), assertionUrn);
    if (assertion != null) {
      if (AssertionType.SQL.equals(assertion.getType())) {
        return isAuthorizedToUpdateSqlAssertionMonitors(entityUrn, context);
      }
      // User is authorized - non sensitive assertion.
      return isAuthorizedToUpdateEntityMonitors(entityUrn, context);
    }
    throw new DataHubGraphQLException(
        String.format("Assertion with urn %s does not exist!", assertionUrn),
        DataHubGraphQLErrorCode.BAD_REQUEST);
  }
}
