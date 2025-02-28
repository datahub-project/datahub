package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils.MAX_ASSERTIONS_TO_RUN_ON_DEMAND;
import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils.extractStringMapEntryInputList;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Submits a group of assertions to be run by the Monitors Service. */
public class RunAssertionsResolver implements DataFetcher<CompletableFuture<RunAssertionsResult>> {
  private final MonitorService monitorService;
  private final AssertionService assertionService;

  public RunAssertionsResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final AssertionService assertionService) {
    this.monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<RunAssertionsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final List<Urn> assertionUrns = extractAssertionUrns(environment.getArgument("urns"));
    final Boolean saveResults = environment.getArgumentOrDefault("saveResults", true);
    final Boolean async = environment.getArgumentOrDefault("async", false);
    final Map<String, String> parameters = extractStringMapEntryInputList(environment);

    if (assertionUrns.size() > MAX_ASSERTIONS_TO_RUN_ON_DEMAND) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to run Assertions. Number of assertions to run exceeds the limit of %d.",
              MAX_ASSERTIONS_TO_RUN_ON_DEMAND),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return CompletableFuture.supplyAsync(
        () -> runAssertions(context, assertionUrns, saveResults, parameters, async));
  }

  @Nullable
  private RunAssertionsResult runAssertions(
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> assertionUrns,
      final boolean saveResults,
      @Nonnull final Map<String, String> parameters,
      final boolean async) {
    /* Validate or throw an exception if the user does not have the necessary privileges to run each assertion. */
    validateRunNativeAssertionPrivileges(assertionUrns, context);

    /* Everything is valid, let's run the assertions */
    final Map<Urn, com.linkedin.assertion.AssertionResult> results =
        monitorService.runAssertions(
            assertionUrns,
            !saveResults, // Dry run is the opposite of saveResults.
            parameters,
            async);

    if (async) {
      // If the request is async, we return null to indicate that the result will be available
      // later.
      return null;
    }

    return AssertionUtils.extractRunResults(context, assertionUrns, results);
  }

  private void validateRunNativeAssertionPrivileges(
      @Nonnull final List<Urn> assertionUrns, @Nonnull final QueryContext context) {
    for (Urn assertionUrn : assertionUrns) {
      final AssertionInfo info =
          getAssertionInfo(
              context.getOperationContext(),
              assertionUrn); // TODO: Build a native batch assertions fetcher.

      /* Ensure that we are dealing with a native Assertion. */
      AssertionUtils.validateAssertionSource(assertionUrn, info);

      final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
      if (!AssertionUtils.isAuthorizedToRunAssertion(asserteeUrn, info.getType(), context)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
    }
  }

  @Nonnull
  private List<Urn> extractAssertionUrns(final Object urns) {
    if (urns instanceof List) {
      return ((List<String>) urns).stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    }
    throw new IllegalArgumentException(
        "Failed to run Assertion. URNs must be a list of assertion urns.");
  }

  @Nonnull
  private AssertionInfo getAssertionInfo(
      @Nonnull final OperationContext opContext, @Nonnull final Urn assertionUrn) {
    final AssertionInfo info = assertionService.getAssertionInfo(opContext, assertionUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to run Assertion. Assertion with urn %s does not exist.", assertionUrn));
    }
    return info;
  }
}
