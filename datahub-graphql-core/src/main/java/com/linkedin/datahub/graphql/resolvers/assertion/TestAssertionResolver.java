package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AssertionResult;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.TestAssertionInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAssertionResolver implements DataFetcher<CompletableFuture<AssertionResult>> {
  private final MonitorService _monitorService;

  public TestAssertionResolver(@Nonnull final MonitorService monitorService) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
  }

  @Override
  public CompletableFuture<AssertionResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final TestAssertionInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), TestAssertionInput.class);
    final Urn asserteeUrn = UrnUtils.getUrn(AssertionUtils.getAsserteeUrnFromTestInput(input));
    final Urn connectionUrn = UrnUtils.getUrn(input.getConnectionUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (isAuthorizedToTestAssertion(asserteeUrn, input, context)) {
            switch (input.getType()) {
              case FRESHNESS:
                final com.linkedin.assertion.AssertionResult freshnessResult =
                    _monitorService.testFreshnessAssertion(
                        asserteeUrn,
                        connectionUrn,
                        FreshnessAssertionUtils.createFreshnessAssertionInfo(
                            input.getFreshnessTestInput()),
                        MonitorUtils.createAssertionEvaluationParameters(input.getParameters()));
                return AssertionRunEventMapper.mapResult(context, freshnessResult);
              case VOLUME:
                final com.linkedin.assertion.AssertionResult volumeResult =
                    _monitorService.testVolumeAssertion(
                        asserteeUrn,
                        connectionUrn,
                        VolumeAssertionUtils.createVolumeAssertionInfo(input.getVolumeTestInput()),
                        MonitorUtils.createAssertionEvaluationParameters(input.getParameters()));
                return AssertionRunEventMapper.mapResult(context, volumeResult);
              case SQL:
                final com.linkedin.assertion.AssertionResult sqlResult =
                    _monitorService.testSqlAssertion(
                        asserteeUrn,
                        connectionUrn,
                        SqlAssertionUtils.createSqlAssertionInfo(input.getSqlTestInput()));
                return AssertionRunEventMapper.mapResult(context, sqlResult);
              case FIELD:
                final com.linkedin.assertion.AssertionResult fieldResult =
                    _monitorService.testFieldAssertion(
                        asserteeUrn,
                        connectionUrn,
                        FieldAssertionUtils.createFieldAssertionInfo(input.getFieldTestInput()),
                        MonitorUtils.createAssertionEvaluationParameters(input.getParameters()));
                return AssertionRunEventMapper.mapResult(context, fieldResult);
              case DATA_SCHEMA:
                final com.linkedin.assertion.AssertionResult dataSchemaResult =
                    _monitorService.testSchemaAssertion(
                        asserteeUrn,
                        connectionUrn,
                        SchemaAssertionUtils.createDataSchemaAssertionInfo(
                            input.getSchemaTestInput()),
                        MonitorUtils.createAssertionEvaluationParameters(input.getParameters()));
                return AssertionRunEventMapper.mapResult(context, dataSchemaResult);
              default:
                throw new IllegalArgumentException(
                    "Unsupported assertion type: " + input.getType());
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private boolean isAuthorizedToTestAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final TestAssertionInput input,
      @Nonnull final QueryContext context) {
    // We must be able to both create assertions + monitors.
    if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {
      // Check whether we are allowed to test sensitive monitor types (Custom SQL).
      if (AssertionType.SQL.equals(input.getType())) {
        return MonitorUtils.isAuthorizedToUpdateSqlAssertionMonitors(asserteeUrn, context);
      }
      // User is authorized.
      return MonitorUtils.isAuthorizedToUpdateEntityMonitors(asserteeUrn, context);
    }
    // Unauthorized
    return false;
  }
}
