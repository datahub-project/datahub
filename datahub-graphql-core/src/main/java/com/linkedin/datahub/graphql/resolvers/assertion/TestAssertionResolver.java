package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AssertionResult;
import com.linkedin.datahub.graphql.generated.TestAssertionInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class TestAssertionResolver implements DataFetcher<CompletableFuture<AssertionResult>> {

  private final MonitorService _monitorService;

  public TestAssertionResolver(@Nonnull final MonitorService monitorService) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
  }

  @Override
  public CompletableFuture<AssertionResult> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final TestAssertionInput
        input = ResolverUtils.bindArgument(environment.getArgument("input"), TestAssertionInput.class);
    
    final Urn asserteeUrn = UrnUtils.getUrn(AssertionUtils.getAsserteeUrnFromTestInput(input));
    final Urn connectionUrn = UrnUtils.getUrn(input.getConnectionUrn());

    return CompletableFuture.supplyAsync(() -> {
      if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {
        switch (input.getType()) {
          case SQL:
            final com.linkedin.assertion.AssertionResult sqlResult = _monitorService.testSqlAssertion(
                asserteeUrn,
                connectionUrn,
                SqlAssertionUtils.createSqlAssertionInfo(input.getSqlTestInput())
            );
            return AssertionRunEventMapper.mapResult(sqlResult);
          case FIELD:
            final com.linkedin.assertion.AssertionResult fieldResult = _monitorService.testFieldAssertion(
                asserteeUrn,
                connectionUrn,
                FieldAssertionUtils.createFieldAssertionInfo(input.getFieldTestInput()),
                MonitorUtils.createAssertionEvaluationParameters(input.getParameters())
            );
            return AssertionRunEventMapper.mapResult(fieldResult);
          default:
            throw new IllegalArgumentException("Unsupported assertion type: " + input.getType());
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}