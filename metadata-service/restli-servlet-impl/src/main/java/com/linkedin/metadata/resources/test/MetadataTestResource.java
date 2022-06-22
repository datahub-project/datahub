package com.linkedin.metadata.resources.test;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTaskTemplate;
import com.linkedin.test.BatchedTestResults;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestResultsMap;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_URN;
import static com.linkedin.metadata.resources.restli.RestliConstants.PARAM_URNS;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiSimpleResource(name = "test", namespace = "com.linkedin.test")
public class MetadataTestResource extends SimpleResourceTaskTemplate<TestInfo> {

  private static final String ACTION_EVALUATE = "evaluate";
  private static final String ACTION_BATCH_EVALUATE = "batchEvaluate";
  private static final String PARAM_TESTS = "tests";
  private static final String PARAM_PUSH = "push";

  @Inject
  @Named("testEngine")
  private TestEngine _testEngine;

  @Action(name = ACTION_EVALUATE)
  @Nonnull
  @WithSpan
  public Task<TestResults> evaluate(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_TESTS) @Optional @Nullable String[] testUrns,
      @ActionParam(PARAM_PUSH) @Optional @Nullable Boolean shouldPush) throws URISyntaxException {
    log.debug("Evaluate tests {} for entity {}", Arrays.toString(testUrns), urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    final List<Urn> tests =
        testUrns == null ? null : Arrays.stream(testUrns).map(UrnUtils::getUrn).collect(Collectors.toList());
    return RestliUtil.toTask(() -> {
      if (tests == null) {
        return _testEngine.evaluateTestsForEntity(urn, shouldPush != null && shouldPush);
      } else {
        return _testEngine.evaluateTests(urn, tests, shouldPush != null && shouldPush);
      }
    }, MetricRegistry.name(this.getClass(), "evaluate"));
  }

  @Action(name = ACTION_BATCH_EVALUATE)
  @Nonnull
  @WithSpan
  public Task<BatchedTestResults> batchEvaluate(@ActionParam(PARAM_URNS) @Nonnull String[] urnStrs,
      @ActionParam(PARAM_TESTS) @Optional @Nullable String[] testUrns,
      @ActionParam(PARAM_PUSH) @Optional @Nullable Boolean shouldPush) throws URISyntaxException {
    log.debug("Evaluate tests {} for entity {}", Arrays.toString(testUrns), urnStrs);

    final List<Urn> urns = Arrays.stream(urnStrs).map(UrnUtils::getUrn).collect(Collectors.toList());
    final List<Urn> tests =
        testUrns == null ? null : Arrays.stream(testUrns).map(UrnUtils::getUrn).collect(Collectors.toList());
    return RestliUtil.toTask(() -> {
      BatchedTestResults results = new BatchedTestResults().setResults(new TestResultsMap());
      if (tests == null) {
        _testEngine.batchEvaluateTestsForEntities(urns, shouldPush != null && shouldPush)
            .forEach((urn, result) -> results.getResults().put(urn.toString(), result));
      } else {
        _testEngine.batchEvaluateTests(urns, tests, shouldPush != null && shouldPush)
            .forEach((urn, result) -> results.getResults().put(urn.toString(), result));
      }
      return results;
    }, MetricRegistry.name(this.getClass(), "batchEvaluate"));
  }
}
