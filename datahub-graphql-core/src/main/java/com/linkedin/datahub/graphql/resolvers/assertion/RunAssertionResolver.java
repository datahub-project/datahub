package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils.extractStringMapEntryInputList;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AssertionResult;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Submits an assertion to be run by the Monitors Service. */
public class RunAssertionResolver implements DataFetcher<CompletableFuture<AssertionResult>> {
  private final MonitorService monitorService;
  private final AssertionService assertionService;

  public RunAssertionResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final AssertionService assertionService) {
    this.monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Nonnull
  @Override
  public CompletableFuture<AssertionResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final Urn assertionUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Boolean saveResult = environment.getArgumentOrDefault("saveResult", true);
    final Map<String, String> parameters = extractStringMapEntryInputList(environment);
    final Boolean async = environment.getArgumentOrDefault("async", false);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> runAssertion(context, assertionUrn, saveResult, parameters, async),
        this.getClass().getSimpleName(),
        "get");
  }

  @Nullable
  private AssertionResult runAssertion(
      @Nonnull final QueryContext context,
      @Nonnull final Urn assertionUrn,
      final boolean saveResult,
      @Nonnull final Map<String, String> parameters,
      final boolean async) {
    /* Validate or throw an exception if the user does not have the necessary privileges to run each assertion. */
    validateRunNativeAssertionPrivileges(assertionUrn, context);

    final Map<Urn, com.linkedin.assertion.AssertionResult> results =
        monitorService.runAssertions(
            ImmutableList.of(assertionUrn),
            !saveResult, // Dry run is the opposite of saveResult.
            parameters,
            async);

    if (async) {
      // If the request is async, we return null to indicate that the result will be available
      // later.
      return null;
    }

    final RunAssertionsResult extractedResult =
        AssertionUtils.extractRunResults(context, ImmutableList.of(assertionUrn), results);

    return extractedResult.getResults().get(0).getResult();
  }

  private void validateRunNativeAssertionPrivileges(
      @Nonnull final Urn assertionUrn, @Nonnull final QueryContext context) {
    final AssertionInfo info = getAssertionInfo(context.getOperationContext(), assertionUrn);

    /* Ensure that we are dealing with a native Assertion. */
    AssertionUtils.validateAssertionSource(assertionUrn, info);

    final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    if (!AssertionUtils.isAuthorizedToRunAssertion(asserteeUrn, info.getType(), context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }

  @Nonnull
  private AssertionInfo getAssertionInfo(
      @Nonnull final OperationContext opContext, @Nonnull final Urn assertionUrn) {
    final AssertionInfo info = assertionService.getAssertionInfo(opContext, assertionUrn);
    if (info == null) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to run Assertion. Assertion with urn %s does not exist.", assertionUrn),
          DataHubGraphQLErrorCode.NOT_FOUND);
    }
    return info;
  }
}
