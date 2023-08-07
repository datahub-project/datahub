package com.linkedin.datahub.upgrade.test;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.BatchTestRunStatus;
import com.linkedin.test.TestResults;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EvaluateTestsStep implements UpgradeStep {

  private static final String ELASTIC_TIMEOUT = System.getenv()
      .getOrDefault(EvaluateTests.ELASTIC_TIMEOUT_ENV_NAME,
          "5m");

  private final EntityClient _entityClient;
  private final EntitySearchService _entitySearchService;
  private final TestEngine _testEngine;
  private final ExecutorService _executorService;
  private final Authentication _systemAuthentication;

  public EvaluateTestsStep(
      @Nonnull EntityClient entityClient,
      @Nonnull EntitySearchService entitySearchService,
      @Nonnull TestEngine testEngine,
      @Nonnull Authentication systemAuthentication) {
    _entityClient = entityClient;
    _entitySearchService = entitySearchService;
    _testEngine = testEngine;
    _systemAuthentication = systemAuthentication;

    int numThreads = Integer.parseInt(System.getenv().getOrDefault(EvaluateTests.EXECUTOR_POOL_SIZE,
        String.valueOf(Runtime.getRuntime().availableProcessors() + 1)));
    _executorService = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  public String id() {
    return "EvaluateTests";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting to evaluate tests...");
      BatchTestResultAggregator resultAggregator = new BatchTestResultAggregator();

      try {
        // Force a synchronous initial load to populate the test cache.
        _testEngine.loadTests();

        int batchSize =
            context.parsedArgs().getOrDefault("BATCH_SIZE", Optional.empty()).map(Integer::parseInt).orElse(1000);

        Set<String> entityTypesToEvaluate = new HashSet<>(_testEngine.getEntityTypesToEvaluate());

        context.report().addLine(String.format("Evaluating tests for entities %s", entityTypesToEvaluate));

        List<Future<Map<Urn, TestResults>>> futures = new ArrayList<>();
        for (String entityType : entityTypesToEvaluate) {
          int batch = 1;
          String nextScrollId = null;
          do {
            context.report().addLine(String.format("Fetching batch %d of %s entities", batch, entityType));
            ScrollResult scrollResult = _entitySearchService.scroll(
                Collections.singletonList(entityType), null, null, batchSize, nextScrollId, ELASTIC_TIMEOUT);
            nextScrollId = scrollResult.getScrollId();
            context.report().addLine(String.format("Processing batch %d of %s entities", batch, entityType));
            List<Urn> entitiesInBatch =
                scrollResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
            final int batchNumber = batch;
            futures.add(_executorService.submit(() -> processBatch(entitiesInBatch, batchNumber, entityType, resultAggregator, context)));
            batch++;
          } while (nextScrollId != null);

          context.report().addLine(String.format("Finished submitting test evaluation for %s entities to worker pool.", entityType));
        }

        for (Future<Map<Urn, TestResults>> results : futures) {
          // Wait for processing, we don't actually use the result currently for anything for now, so treat as void
          try {
            results.get();
          } catch (InterruptedException | ExecutionException e) {
            context.report().addLine("Reading interrupted, not able to finish processing.");
            throw new RuntimeException(e);
          }
        }
        context.report().addLine("Finished evaluating tests for all entities");
        reportTestRunResults(resultAggregator.getTestResultSummaries().values());
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to complete test evaluation! Caught an exception while running the test. This may mean that the test run was incomplete.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
    };
  }

  private Map<Urn, TestResults> processBatch(
      List<Urn> entitiesInBatch,
      int batchNumber,
      String entityType,
      BatchTestResultAggregator resultAggregator,
      UpgradeContext context) {
    {
      Map<Urn, TestResults> result;
      try {
        result = _testEngine.batchEvaluateTestsForEntities(entitiesInBatch, TestEngine.EvaluationMode.DEFAULT);
        context.report()
            .addLine(String.format("Pushed %d test results for batch %d of %s entities", result.size(), batchNumber,
                entityType));
        incrementBatchCounters(entitiesInBatch, result, resultAggregator);
        return result;
      } catch (Exception e) {
        context.report().addLine(String.format("Error while processing batch %d of %s entities", batchNumber, entityType));
        log.error("Error while processing batch {} of {} entities", batchNumber, entityType, e);
      }
      return null;
    }
  }

  /**
   * Increment the counters used to report results for the metadata test
   */
  private void incrementBatchCounters(
      @Nonnull final List<Urn> entitiesInBatch, 
      @Nonnull final Map<Urn, TestResults> results, 
      @Nonnull final BatchTestResultAggregator resultAggregator) {
    for (Urn entityUrn: entitiesInBatch) {
      TestResults result = results.get(entityUrn);
      if (result != null) {
        if (result.hasPassing()) {
          result.getPassing().forEach(passingTest ->
              resultAggregator.incrementPass(passingTest.getTest())
          );
        }
        if (result.hasFailing()) {
          result.getFailing().forEach(failingTest ->
              resultAggregator.incrementFail(failingTest.getTest())
          );
        }
      }
    }
  }

  /**
   * In this method we produce a BatchTestRunResult aspect for each test that should have been executed
   * by this process.
   *
   * To do so, we also issue a query to count the number of entities with a given run result.
   */
  private void reportTestRunResults(@Nonnull final Collection<BatchTestResult> results) {
    try {
      final long currentTime = System.currentTimeMillis();
      for (BatchTestResult result : results) {
        reportTestRunResult(result, currentTime);
      }
    } catch (Exception e) {
      // Catch everything so that issuing in reporting never break all tests.
      log.error(
          "Caught exception while attempting to report Metadata Test run results! This may mean that one or more tests is missing results.",
          e);
    }
  }

  private void reportTestRunResult(
      @Nonnull final BatchTestResult result,
      final long currentTime) {
    Urn testUrn = result.getUrn();
    BatchTestRunEvent event = new BatchTestRunEvent();
    event.setTimestampMillis(currentTime);
    event.setStatus(BatchTestRunStatus.COMPLETE);
    event.setResult(new BatchTestRunResult()
        .setPassingCount(result.getPassCount())
        .setFailingCount(result.getFailCount()));
    try {
      _entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              testUrn,
              AcrylConstants.BATCH_TEST_RUN_EVENT_ASPECT_NAME,
              event
          ),
          _systemAuthentication
      );
    } catch (Exception e) {
      log.error("Failed to produce Metadata Test Run Result aspect! This may mean that the results shown in the UI are stale!", e);
    }
  }
}
