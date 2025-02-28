package com.linkedin.datahub.graphql.resolvers.datacontract;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.DataContractResult;
import com.linkedin.datahub.graphql.generated.DataContractResultType;
import com.linkedin.datahub.graphql.generated.RunAssertionResult;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionUtils;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.DataContractService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Returns a result for a Data Contract, and optionally runs native assertions to get it * */
@Slf4j
public class DataContractResultResolver
    implements DataFetcher<CompletableFuture<DataContractResult>> {

  private static final DataContractResult DEFAULT_PASSING_CONTRACT_RESULT =
      new DataContractResult(
          DataContractResultType.PASSING,
          new RunAssertionsResult(0, 0, 0, Collections.emptyList()));

  private final MonitorService monitorService;
  private final AssertionService assertionService;
  private final DataContractService dataContractService;

  public DataContractResultResolver(
      @Nonnull final MonitorService monitorService,
      @Nonnull final AssertionService assertionService,
      @Nonnull final DataContractService dataContractService) {
    this.monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
    this.dataContractService =
        Objects.requireNonNull(dataContractService, "dataContractService is required");
  }

  @Override
  public CompletableFuture<DataContractResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final Urn contractUrn = UrnUtils.getUrn(((DataContract) environment.getSource()).getUrn());
    final Boolean refresh = environment.getArgumentOrDefault("refresh", false);

    return CompletableFuture.supplyAsync(
        () -> getDataContractResult(context, contractUrn, refresh));
  }

  @Nullable
  private DataContractResult getDataContractResult(
      @Nonnull final QueryContext context, @Nonnull final Urn contractUrn, final boolean refresh) {
    final List<Urn> assertionUrns =
        dataContractService.getDataContractAssertionUrns(
            context.getOperationContext(), contractUrn);

    if (assertionUrns == null || assertionUrns.isEmpty()) {
      return DEFAULT_PASSING_CONTRACT_RESULT;
    }

    final RunAssertionsResult results =
        getContractAssertionsResults(context, assertionUrns, refresh);
    return buildContractResult(results);
  }

  private DataContractResult buildContractResult(
      @Nonnull final RunAssertionsResult assertionResults) {

    // Note that ERROR state and INIT state do not count as
    // part of the contract.
    DataContractResultType type =
        assertionResults.getFailingCount() > 0
            ? DataContractResultType.FAILING
            : DataContractResultType.PASSING;

    DataContractResult result = new DataContractResult();
    result.setType(type);
    result.setAssertionResults(assertionResults);

    return result;
  }

  private RunAssertionsResult getContractAssertionsResults(
      @Nonnull final QueryContext context,
      @Nonnull final List<Urn> assertionUrns,
      final boolean refresh) {

    final List<Urn> nativeAssertions = new ArrayList<>();
    final List<RunAssertionResult> runResults = new ArrayList<>();
    for (final Urn assertionUrn : assertionUrns) {
      final AssertionInfo info = getAssertionInfo(context.getOperationContext(), assertionUrn);

      if (info == null) {
        log.warn(
            String.format(
                "Found missing contract assertion with urn %s. Skipping in contract result!",
                assertionUrn));
        continue;
      }

      if (refresh
          && info.hasSource()
          && com.linkedin.assertion.AssertionSourceType.NATIVE.equals(info.getSource().getType())) {
        // If refresh is enabled, add urn to a batch to execute.
        validateRunNativeAssertionPrivileges(info, context);
        nativeAssertions.add(assertionUrn);
      } else {
        // Add the latest run to the result.
        final RunAssertionResult runResult = getLatestRunResult(context, assertionUrn);
        if (runResult != null) {
          runResults.add(runResult);
        } else {
          log.warn(
              String.format(
                  "Did not find any run results for assertion urn %s. Skipping adding to contract result.",
                  assertionUrn));
        }
      }
    }
    if (refresh) {
      RunAssertionsResult nativeResults = refreshNativeAssertions(context, nativeAssertions);
      runResults.addAll(nativeResults.getResults());
    }

    return buildFinalRunResults(runResults);
  }

  @Nonnull
  private RunAssertionsResult buildFinalRunResults(
      @Nonnull final List<RunAssertionResult> runResults) {
    RunAssertionsResult result = new RunAssertionsResult();
    result.setResults(runResults);

    // Compute final counts

    result.setFailingCount(
        (int)
            runResults.stream()
                .filter(
                    r ->
                        r.getResult()
                            .getType()
                            .equals(
                                com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE))
                .count());
    result.setPassingCount(
        (int)
            runResults.stream()
                .filter(
                    r ->
                        r.getResult()
                            .getType()
                            .equals(
                                com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS))
                .count());
    result.setErrorCount(
        (int)
            runResults.stream()
                .filter(
                    r ->
                        r.getResult()
                            .getType()
                            .equals(
                                com.linkedin.datahub.graphql.generated.AssertionResultType.ERROR))
                .count());
    result.setResults(runResults);
    return result;
  }

  private RunAssertionsResult refreshNativeAssertions(
      QueryContext context, List<Urn> assertionUrns) {
    // TODO: Determine whether we'll need to support runtime parameters here.
    final Map<Urn, AssertionResult> results =
        monitorService.runAssertions(assertionUrns, false, Collections.emptyMap(), false);
    return AssertionUtils.extractRunResults(context, assertionUrns, results);
  }

  private void validateRunNativeAssertionPrivileges(
      @Nonnull final AssertionInfo info, @Nonnull final QueryContext context) {
    final Urn asserteeUrn = AssertionUtils.getAsserteeUrnFromInfo(info);
    if (!AssertionUtils.isAuthorizedToRunAssertion(asserteeUrn, info.getType(), context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }

  @Nullable
  private AssertionInfo getAssertionInfo(
      @Nonnull final OperationContext opContext, @Nonnull final Urn assertionUrn) {
    return assertionService.getAssertionInfo(opContext, assertionUrn);
  }

  @Nullable
  private RunAssertionResult getLatestRunResult(
      @Nonnull final QueryContext context, @Nonnull final Urn assertionUrn) {
    final RunAssertionResult result = new RunAssertionResult();

    final Assertion unresolvedAssertion = new Assertion();
    unresolvedAssertion.setUrn(assertionUrn.toString());
    result.setAssertion(unresolvedAssertion);

    final AssertionResult latestResult =
        assertionService.getLatestAssertionRunResult(context.getOperationContext(), assertionUrn);

    if (latestResult == null) {
      return null;
    }

    result.setResult(AssertionRunEventMapper.mapResult(context, latestResult));
    return result;
  }
}
