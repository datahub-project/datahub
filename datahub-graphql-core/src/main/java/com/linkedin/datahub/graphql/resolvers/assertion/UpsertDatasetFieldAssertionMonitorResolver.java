package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.UpsertDatasetFieldAssertionMonitorInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.MonitorMode;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertDatasetFieldAssertionMonitorResolver
    implements DataFetcher<CompletableFuture<Assertion>> {
  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final GraphClient _graphClient;

  public UpsertDatasetFieldAssertionMonitorResolver(
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
    final UpsertDatasetFieldAssertionMonitorInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), UpsertDatasetFieldAssertionMonitorInput.class);

    final Urn entityUrn, monitorUrn, assertionUrn;

    boolean isCreate = maybeAssertionUrn == null;
    if (isCreate) {
      // Create Assertion - only entityUrn is known. Generate new assertionUrn and monitorUrn.
      if (input.getEntityUrn() == null) {
        throw new IllegalArgumentException("Failed to create Assertion. entityUrn is required.");
      }
      entityUrn = UrnUtils.getUrn(input.getEntityUrn());
      assertionUrn = _assertionService.generateAssertionUrn();
      monitorUrn = _monitorService.generateMonitorUrn(entityUrn);
    } else {
      // Update Assertion - only assertionUrn is known. Extract entityUrn and monitorUrn using
      // assertionUrn.
      assertionUrn = UrnUtils.getUrn(maybeAssertionUrn);
      entityUrn = getEntityUrnForFieldAssertion(assertionUrn, input);
      monitorUrn = getMonitorUrnForAssertion(_graphClient, assertionUrn);
    }

    // upsert assertion
    return CompletableFuture.supplyAsync(
        () -> {
          // Check whether the current user is allowed to update the assertion.
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, entityUrn)
              && isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {
            // First upsert the assertion.
            _assertionService.upsertDatasetFieldAssertion(
                assertionUrn,
                entityUrn,
                FieldAssertionUtils.createFieldAssertionInfo(input),
                input.getActions() != null
                    ? AssertionUtils.createAssertionActions(input.getActions())
                    : null,
                context.getAuthentication());

            // Then, upsert the monitor
            try {
              _monitorService.upsertAssertionMonitor(
                  monitorUrn,
                  assertionUrn,
                  entityUrn,
                  createCronSchedule(input.getEvaluationSchedule()),
                  createFieldAssertionEvaluationParameters(input.getEvaluationParameters()),
                  MonitorMode.valueOf(input.getMode().toString()),
                  input.getExecutorId(),
                  context.getAuthentication());
            } catch (Exception e) {
              log.error("Failed to upsert Assertion monitor!", e);
              if (isCreate) {
                log.info(
                    String.format(
                        "Deleting partially created native assertion with urn %s ", assertionUrn));
                _assertionService.tryDeleteAssertion(assertionUrn, context.getAuthentication());
              }
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to upsert Assertion Monitor! An unknown error occurred. Error: %s",
                      e.getMessage()),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }

            // Then, return the new assertion
            return AssertionMapper.map(
                _assertionService.getAssertionEntityResponse(
                    assertionUrn, context.getAuthentication()));
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private Urn getEntityUrnForFieldAssertion(
      Urn assertionUrn, UpsertDatasetFieldAssertionMonitorInput input) {
    final Urn entityUrn;
    final AssertionInfo info = _assertionService.getAssertionInfo(assertionUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }
    if (info.getType() != AssertionType.FIELD) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update Assertion. Assertion with urn %s is not a field assertion.",
              assertionUrn));
    }
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
