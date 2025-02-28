package com.linkedin.metadata.resources.test;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.linkedin.metadata.authorization.PoliciesConfig.MANAGE_TESTS_PRIVILEGE;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_URN;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_URNS;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTaskTemplate;
import com.linkedin.test.BatchedTestResults;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestResultsMap;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** Single unified resource for fetching, updating, searching, & browsing DataHub entities */
@Slf4j
@RestLiSimpleResource(name = "test", namespace = "com.linkedin.test")
public class MetadataTestResource extends SimpleResourceTaskTemplate<TestInfo> {

  private static final String ACTION_EVALUATE = "evaluate";
  private static final String ACTION_TEST_EVALUATE = "testEvaluate";
  private static final String ACTION_BATCH_EVALUATE = "batchEvaluate";
  private static final String PARAM_TESTS = "tests";
  private static final String PARAM_PUSH = "push";
  private static final int MAX_TEST_RESULT_SIZE = 1000;

  @Inject
  @Named("testEngine")
  private TestEngine _testEngine;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

    @Inject
    @Named("authorizerChain")
    private Authorizer _authorizer;

  @Action(name = ACTION_EVALUATE)
  @Nonnull
  @WithSpan
  public Task<TestResults> evaluate(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_TESTS) @Optional @Nullable String[] testUrns,
      @ActionParam(PARAM_PUSH) @Optional @Nullable Boolean shouldPush)
      throws URISyntaxException {
    log.debug("Evaluate tests {} for entity {}", Arrays.toString(testUrns), urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    final Set<Urn> tests =
        testUrns == null
            ? null
            : Arrays.stream(testUrns).map(UrnUtils::getUrn).collect(Collectors.toSet());
    return RestliUtils.toTask(systemOperationContext,
        () -> {

            Authentication auth = AuthenticationContext.getAuthentication();
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_EVALUATE, List.of()), _authorizer, auth, true);

            if (!isAPIAuthorized(
                    opContext,
                    MANAGE_TESTS_PRIVILEGE)) {
                throw new RestLiServiceException(
                        HttpStatus.S_403_FORBIDDEN, "User is unauthorized for tests");
            }

            if (tests == null) {
            return _testEngine.evaluateTests(opContext,
                urn,
                shouldPush != null && shouldPush
                    ? TestEngine.EvaluationMode.DEFAULT
                    : TestEngine.EvaluationMode.EVALUATE_ONLY);
          }
          return _testEngine.evaluateTestUrns(opContext,
              urn,
              tests,
              shouldPush != null && shouldPush
                  ? TestEngine.EvaluationMode.DEFAULT
                  : TestEngine.EvaluationMode.EVALUATE_ONLY);
        },
        MetricRegistry.name(this.getClass(), "evaluate"));
  }

  @Action(name = ACTION_BATCH_EVALUATE)
  @Nonnull
  @WithSpan
  public Task<BatchedTestResults> batchEvaluate(
      @ActionParam(PARAM_URNS) @Nonnull String[] urnStrs,
      @ActionParam(PARAM_TESTS) @Optional @Nullable String[] testUrns,
      @ActionParam(PARAM_PUSH) @Optional @Nullable Boolean shouldPush)
      throws URISyntaxException {
    log.debug("Evaluate tests {} for entity {}", Arrays.toString(testUrns), urnStrs);

    final Set<Urn> urns =
        Arrays.stream(urnStrs).map(UrnUtils::getUrn).collect(Collectors.toSet());
    final Set<Urn> tests =
        testUrns == null
            ? null
            : Arrays.stream(testUrns).map(UrnUtils::getUrn).collect(Collectors.toSet());
    return RestliUtils.toTask(systemOperationContext,
        () -> {

            Authentication auth = AuthenticationContext.getAuthentication();
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(), ACTION_EVALUATE, List.of()), _authorizer, auth, true);

            if (!isAPIAuthorized(
                    opContext,
                    MANAGE_TESTS_PRIVILEGE)) {
                throw new RestLiServiceException(
                        HttpStatus.S_403_FORBIDDEN, "User is unauthorized for tests");
            }

            BatchedTestResults results = new BatchedTestResults().setResults(new TestResultsMap());
          if (tests == null) {
            _testEngine
                .evaluateTests(opContext,
                    urns,
                    shouldPush != null && shouldPush
                        ? TestEngine.EvaluationMode.DEFAULT
                        : TestEngine.EvaluationMode.EVALUATE_ONLY)
                .forEach((urn, result) -> results.getResults().put(urn.toString(), result));
          } else {
            _testEngine
                .evaluateTestUrns(opContext,
                    urns,
                    tests,
                    shouldPush != null && shouldPush
                        ? TestEngine.EvaluationMode.DEFAULT
                        : TestEngine.EvaluationMode.EVALUATE_ONLY)
                .forEach((urn, result) -> results.getResults().put(urn.toString(), result));
          }
          return results;
        },
        MetricRegistry.name(this.getClass(), "batchEvaluate"));
  }

  @Action(name = ACTION_TEST_EVALUATE)
  @Nonnull
  @WithSpan
  public Task<BatchedTestResults> evaluateSingleTest(
      @ActionParam(PARAM_URN) @Nonnull String testUrnStr,
      @ActionParam(PARAM_PUSH) @Optional @Nullable Boolean shouldPush)
      throws URISyntaxException {
    log.info("Evaluate single test called with urn: {}, shouldPush: {}", testUrnStr, shouldPush);
    final Urn testUrn = Urn.createFromString(testUrnStr);
    return RestliUtils.toTask(systemOperationContext, () -> {

        Authentication auth = AuthenticationContext.getAuthentication();
        final OperationContext opContext = OperationContext.asSession(
                systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(), ACTION_EVALUATE, List.of()), _authorizer, auth, true);

        if (!isAPIAuthorized(
                opContext,
                MANAGE_TESTS_PRIVILEGE)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized for tests");
        }



        TestResultsMap testResultsMap = new TestResultsMap();
          _testEngine.evaluateSingleTest(opContext, testUrn,
              shouldPush != null && shouldPush ? TestEngine.EvaluationMode.DEFAULT
                  : TestEngine.EvaluationMode.EVALUATE_ONLY)
              .forEach((urn, result) -> testResultsMap.put(urn.toString(), result));
          if (testResultsMap.size() > MAX_TEST_RESULT_SIZE) {
              log.warn(
                  String.format(
                      "TestResultsMap exceeds the max size and would cause TooLongHttpContentException. Trimming results from %s to %s", testResultsMap.size(), MAX_TEST_RESULT_SIZE));
              TestResultsMap trimmedTestResultsMap = new TestResultsMap();
              testResultsMap.entrySet().stream().limit(MAX_TEST_RESULT_SIZE).forEach(entry -> {
                  trimmedTestResultsMap.put(entry.getKey(), entry.getValue());
              });
              return new BatchedTestResults().setResults(trimmedTestResultsMap);
          }
          return new BatchedTestResults().setResults(testResultsMap);
        },
    MetricRegistry.name(this.getClass(), "evaluateSingleTest"));
  }

}