package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.SchemaAssertionUtils.mapEvaluationParameters;
import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.UpsertDatasetSchemaAssertionMonitorInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorMode;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertDatasetSchemaAssertionMonitorResolver
    implements DataFetcher<CompletableFuture<Assertion>> {

  // hourly run
  private final String HOURLY_CRON_SCHEDULE = "0 */6 * * *";
  private final CronSchedule DEFAULT_CRON_SCHEDULE =
      new CronSchedule().setCron(HOURLY_CRON_SCHEDULE).setTimezone("UTC");

  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final GraphClient _graphClient;
  final LongSupplier _timeProvider;

  public UpsertDatasetSchemaAssertionMonitorResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final GraphClient graphClient,
      @Nonnull final LongSupplier timeProvider) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
    _graphClient = Objects.requireNonNull(graphClient, "graphClient is required");
    _timeProvider = Objects.requireNonNull(timeProvider, "timeProvider is required");
  }

  public UpsertDatasetSchemaAssertionMonitorResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final GraphClient graphClient) {
    this(assertionService, monitorService, graphClient, () -> System.currentTimeMillis());
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String maybeAssertionUrn = environment.getArgument("assertionUrn");
    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final UpsertDatasetSchemaAssertionMonitorInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpsertDatasetSchemaAssertionMonitorInput.class);

    final Urn entityUrn, monitorUrn, assertionUrn;
    final AssertionSource assertionSource;
    final String appSource = ResolverUtils.resolveAppSource(context);

    boolean isCreate = maybeAssertionUrn == null;
    if (isCreate) {
      // Create Assertion - only entityUrn is known. Generate new assertionUrn and monitorUrn.
      if (input.getEntityUrn() == null) {
        throw new IllegalArgumentException("Failed to create Assertion. entityUrn is required.");
      }
      assertionUrn = _assertionService.generateAssertionUrn();
      assertionSource = new AssertionSource();
      assertionSource.setCreated(
          new AuditStamp().setActor(actorUrn).setTime(_timeProvider.getAsLong()));
      assertionSource.setType(AssertionSourceType.NATIVE);
      entityUrn = UrnUtils.getUrn(input.getEntityUrn());
      log.debug(String.format("Creating assertion with urn %s ...", assertionUrn));
      monitorUrn = _monitorService.generateMonitorUrn(entityUrn);
    } else {
      // Update Assertion - only assertionUrn is known. Extract entityUrn and monitorUrn using
      // assertionUrn.
      log.debug(String.format("Updating assertion with urn %s ...", maybeAssertionUrn));
      assertionUrn = UrnUtils.getUrn(maybeAssertionUrn);
      AssertionInfo info =
          getAssertionInfoForSchemaAssertion(context.getOperationContext(), assertionUrn);
      assertionSource = info.getSource();
      entityUrn = getEntityUrnForSchemaAssertion(assertionUrn, info, input);
      monitorUrn = getMonitorUrnForAssertionOrThrow(_graphClient, assertionUrn);
    }

    // upsert assertion
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Check whether the current user is allowed to update the assertion.
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, entityUrn)
              && isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {
            // First upsert the assertion.
            _assertionService.upsertDatasetSchemaAssertion(
                context.getOperationContext(),
                assertionUrn,
                entityUrn,
                input.getDescription(),
                SchemaAssertionCompatibility.valueOf(
                    input.getAssertion().getCompatibility().toString()),
                SchemaAssertionUtils.createSchemaMetadata(input.getAssertion().getFields()),
                input.getActions() != null
                    ? AssertionUtils.createAssertionActions(input.getActions())
                    : null,
                assertionSource,
                appSource);

            // Then, upsert the monitor -> In our case the monitor is not a scheduled monitor. It's
            // an event based
            // monitor.
            try {
              _monitorService.upsertAssertionMonitor(
                  context.getOperationContext(),
                  monitorUrn,
                  assertionUrn,
                  entityUrn,
                  input.getEvaluationSchedule() != null
                      ? createCronSchedule(input.getEvaluationSchedule())
                      : DEFAULT_CRON_SCHEDULE,
                  mapEvaluationParameters(input.getEvaluationParameters()),
                  MonitorMode.valueOf(input.getMode().toString()),
                  input.getExecutorId(),
                  appSource,
                  null);
            } catch (Exception e) {
              log.error("Failed to upsert Assertion monitor!", e);
              if (isCreate) {
                log.info(
                    String.format(
                        "Deleting partially created native assertion with urn %s ", assertionUrn));
                _assertionService.tryDeleteAssertion(context.getOperationContext(), assertionUrn);
              }
              if (e.getMessage() != null
                  && e.getMessage()
                      .contains(AcrylConstants.MONITOR_LIMIT_EXCEEDED_ERROR_MESSAGE_PREFIX)) {
                throw new DataHubGraphQLException(
                    String.format("Maximum number of monitors reached. %s", e.getMessage()),
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to upsert Assertion Monitor! An unknown error occurred. Error: %s",
                      e.getMessage()),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }

            // Then, return the new assertion
            return AssertionMapper.map(
                context,
                _assertionService.getAssertionEntityResponse(
                    context.getOperationContext(), assertionUrn));
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private AssertionInfo getAssertionInfoForSchemaAssertion(
      @Nonnull OperationContext opContext, Urn assertionUrn) {
    final AssertionInfo info = _assertionService.getAssertionInfo(opContext, assertionUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }
    if (info.getType() != AssertionType.DATA_SCHEMA) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not a freshness assertion.",
              assertionUrn));
    }
    return info;
  }

  private Urn getEntityUrnForSchemaAssertion(
      Urn assertionUrn, AssertionInfo info, UpsertDatasetSchemaAssertionMonitorInput input) {
    final Urn entityUrn;
    entityUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    if (input.getEntityUrn() != null && !input.getEntityUrn().equals(entityUrn.toString())) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not linked Entity with urn %s.",
              assertionUrn, input.getEntityUrn()));
    }
    return entityUrn;
  }
}
