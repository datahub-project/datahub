package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.UpsertDatasetSqlAssertionMonitorInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.MonitorMode;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertDatasetSqlAssertionMonitorResolver
    implements DataFetcher<CompletableFuture<Assertion>> {
  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final GraphClient _graphClient;

  public UpsertDatasetSqlAssertionMonitorResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final GraphClient graphClient) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
    _graphClient = Objects.requireNonNull(graphClient, "graphClient is required");
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String maybeAssertionUrn = environment.getArgument("assertionUrn");
    final UpsertDatasetSqlAssertionMonitorInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpsertDatasetSqlAssertionMonitorInput.class);

    final Urn entityUrn, monitorUrn, assertionUrn;
    final AssertionSource assertionSource;

    boolean isCreate = maybeAssertionUrn == null;
    if (isCreate) {
      // Create Assertion - only entityUrn is known. Generate new assertionUrn and monitorUrn.
      if (input.getEntityUrn() == null) {
        throw new IllegalArgumentException("Failed to create Assertion. entityUrn is required.");
      }
      assertionUrn = _assertionService.generateAssertionUrn();
      assertionSource = null;
      entityUrn = UrnUtils.getUrn(input.getEntityUrn());

      monitorUrn = _monitorService.generateMonitorUrn(entityUrn);
    } else {
      // Update Assertion - only assertionUrn is known. Extract entityUrn and monitorUrn using
      // assertionUrn.
      assertionUrn = UrnUtils.getUrn(maybeAssertionUrn);
      AssertionInfo info =
          getAssertionInfoForSQLAssertion(context.getOperationContext(), assertionUrn);
      assertionSource = info.getSource();
      entityUrn = getEntityUrnForSqlAssertion(assertionUrn, info, input);

      monitorUrn = getMonitorUrnForAssertionOrThrow(_graphClient, assertionUrn);
    }

    // upsert assertion
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Check whether the current user is allowed to update the assertion.
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, entityUrn)
              && isAuthorizedToUpdateSqlAssertionMonitors(entityUrn, context)) {
            // First upsert the assertion.
            _assertionService.upsertDatasetSqlAssertion(
                context.getOperationContext(),
                assertionUrn,
                entityUrn,
                SqlAssertionType.valueOf(input.getType().toString()),
                input.getDescription(),
                SqlAssertionUtils.createSqlAssertionInfo(input),
                input.getActions() != null
                    ? AssertionUtils.createAssertionActions(input.getActions())
                    : null,
                assertionSource);

            // Then, upsert the monitor
            try {
              _monitorService.upsertAssertionMonitor(
                  context.getOperationContext(),
                  monitorUrn,
                  assertionUrn,
                  entityUrn,
                  createCronSchedule(input.getEvaluationSchedule()),
                  new AssertionEvaluationParameters()
                      .setType(AssertionEvaluationParametersType.DATASET_SQL),
                  MonitorMode.valueOf(input.getMode().toString()),
                  input.getExecutorId());
            } catch (Exception e) {
              log.error("Failed to upsert Assertion monitor!", e);
              if (isCreate) {
                log.info(
                    String.format(
                        "Deleting partially created native assertion with urn %s ", assertionUrn));
                _assertionService.tryDeleteAssertion(context.getOperationContext(), assertionUrn);
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

  private AssertionInfo getAssertionInfoForSQLAssertion(
      @Nonnull OperationContext opContext, Urn assertionUrn) {
    final AssertionInfo info = _assertionService.getAssertionInfo(opContext, assertionUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }
    if (info.getType() != AssertionType.SQL) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not an SQL assertion.",
              assertionUrn));
    }
    return info;
  }

  private Urn getEntityUrnForSqlAssertion(
      Urn assertionUrn, AssertionInfo info, UpsertDatasetSqlAssertionMonitorInput input) {
    final Urn entityUrn;

    entityUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    if (input.getEntityUrn() != null && !input.getEntityUrn().equals(entityUrn.toString())) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not linked Entity with urn %s.",
              assertionUrn, input.getEntityUrn()));
    }

    input.setEntityUrn(entityUrn.toString());
    return entityUrn;
  }
}
