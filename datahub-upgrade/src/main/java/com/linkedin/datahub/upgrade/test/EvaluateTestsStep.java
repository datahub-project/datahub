package com.linkedin.datahub.upgrade.test;

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
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.BatchTestRunStatus;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.PartitionSpec;
import com.linkedin.timeseries.PartitionType;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EvaluateTestsStep implements UpgradeStep {

  private static final String ELASTIC_TIMEOUT =
      System.getenv().getOrDefault(EvaluateTests.ELASTIC_TIMEOUT_ENV_NAME, "5m");

  private final OperationContext systemOpContext;
  private final EntityClient entityClient;
  private final EntitySearchService _entitySearchService;
  private final TestEngine testEngine;
  private final ExecutorService executorService;
  private final TestEngine.EvaluationMode evaluationMode;
  private final String runId;

  public EvaluateTestsStep(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityClient entityClient,
      @Nonnull EntitySearchService entitySearchService,
      @Nonnull TestEngine testEngine) {
    this.systemOpContext = systemOpContext;
    this.entityClient = entityClient;
    _entitySearchService = entitySearchService;
    this.testEngine = testEngine;
    this.runId = String.format("cron-%s", Instant.now());

    int numThreads =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    EvaluateTests.EXECUTOR_POOL_SIZE,
                    String.valueOf(Runtime.getRuntime().availableProcessors() + 1)));
    int queueSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    EvaluateTests.EXECUTOR_QUEUE_SIZE,
                    String.valueOf((Runtime.getRuntime().availableProcessors() + 1) * 2)));
    executorService =
        new ThreadPoolExecutor(
            numThreads,
            numThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize));
    String envEvalMode = System.getenv(EvaluateTests.EVALUATION_MODE);
    evaluationMode = TestEngine.EvaluationMode.getEvaluationMode(envEvalMode);
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
        int batchSize =
            context
                .parsedArgs()
                .getOrDefault("batchSize", Optional.empty())
                .map(Integer::parseInt)
                .orElse(1000);
        int batchDelayMs =
            context
                .parsedArgs()
                .getOrDefault("batchDelayMs", Optional.empty())
                .map(Integer::parseInt)
                .orElse(250);

        Set<String> entityTypesToEvaluate = new HashSet<>(testEngine.getEntityTypesToEvaluate());

        context
            .report()
            .addLine(String.format("Evaluating tests for entities %s", entityTypesToEvaluate));

        List<Future<Map<Urn, TestResults>>> futures = new ArrayList<>();
        for (String entityType : entityTypesToEvaluate) {
          int batch = 1;
          String nextScrollId = null;
          do {
            context
                .report()
                .addLine(String.format("Fetching batch %d of %s entities", batch, entityType));
            ScrollResult scrollResult =
                _entitySearchService.scroll(
                    systemOpContext,
                    Collections.singletonList(entityType),
                    null,
                    null,
                    batchSize,
                    nextScrollId,
                    ELASTIC_TIMEOUT,
                    null);
            nextScrollId = scrollResult.getScrollId();
            context
                .report()
                .addLine(String.format("Processing batch %d of %s entities", batch, entityType));
            Set<Urn> entitiesInBatch =
                scrollResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toSet());
            final int batchNumber = batch;
            futures.add(
                executorService.submit(
                    () ->
                        processBatch(
                            entitiesInBatch,
                            batchNumber,
                            batchDelayMs,
                            entityType,
                            resultAggregator,
                            context)));
            batch++;
            if (batchDelayMs > 0) {
              TimeUnit.MILLISECONDS.sleep(batchDelayMs);
            }
          } while (nextScrollId != null);

          context
              .report()
              .addLine(
                  String.format(
                      "Finished submitting test evaluation for %s entities to worker pool.",
                      entityType));
        }

        for (Future<Map<Urn, TestResults>> results : futures) {
          // Wait for processing, we don't actually use the result currently for anything for now,
          // so treat as void
          try {
            results.get();
          } catch (InterruptedException | ExecutionException e) {
            context.report().addLine("Reading interrupted, not able to finish processing.");
            throw new RuntimeException(e);
          }
        }
        context.report().addLine("Finished evaluating tests for all entities");
        reportTestRunResults(resultAggregator.getTestResultSummaries().values());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error(
            "Failed to complete test evaluation! Caught an exception while running the test. This may mean that the test run was incomplete.",
            e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private Map<Urn, TestResults> processBatch(
      Set<Urn> entitiesInBatch,
      int batchNumber,
      int batchDelayMs,
      String entityType,
      BatchTestResultAggregator resultAggregator,
      UpgradeContext context)
      throws InterruptedException {
    {
      Map<Urn, TestResults> result;
      try {
        result =
            testEngine.evaluateTests(
                systemOpContext, entitiesInBatch, TestEngine.EvaluationMode.DEFAULT);
        context
            .report()
            .addLine(
                String.format(
                    "Pushed %d test results for batch %d of %s entities",
                    result.size(), batchNumber, entityType));
        incrementBatchCounters(result, resultAggregator);
        return result;
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Error while processing batch %d of %s entities", batchNumber, entityType));
        log.error("Error while processing batch {} of {} entities", batchNumber, entityType, e);
      } finally {
        if (batchDelayMs > 0) {
          TimeUnit.MILLISECONDS.sleep(batchDelayMs);
        }
      }
      return null;
    }
  }

  /** Increment the counters used to report results for the metadata test */
  private void incrementBatchCounters(
      @Nonnull final Map<Urn, TestResults> results,
      @Nonnull final BatchTestResultAggregator resultAggregator) {

    results.values().stream()
        .flatMap(
            r ->
                Stream.concat(
                    r.hasPassing() ? r.getPassing().stream() : Stream.empty(),
                    r.hasFailing() ? r.getFailing().stream() : Stream.empty()))
        .collect(
            Collectors.groupingBy(r -> Pair.of(r.getTest(), r.getType()), Collectors.counting()))
        .forEach(
            (key, count) -> {
              if (TestResultType.SUCCESS.equals(key.getSecond())) {
                resultAggregator.incrementPass(key.getKey(), count);
              } else {
                resultAggregator.incrementFail(key.getKey(), count);
              }
            });
  }

  /**
   * In this method we produce a BatchTestRunResult aspect for each test that should have been
   * executed by this process.
   *
   * <p>To do so, we also issue a query to count the number of entities with a given run result.
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

  private void reportTestRunResult(@Nonnull final BatchTestResult result, final long currentTime) {
    Urn testUrn = result.getUrn();
    BatchTestRunEvent event = new BatchTestRunEvent();
    event.setTimestampMillis(currentTime);
    event.setStatus(BatchTestRunStatus.COMPLETE);
    PartitionSpec partitionSpec =
        new PartitionSpec().setType(PartitionType.FULL_TABLE).setPartition("FULL");
    event.setPartitionSpec(partitionSpec);
    event.setResult(
        new BatchTestRunResult()
            .setPassingCount(result.getPassCount())
            .setFailingCount(result.getFailCount()));
    try {
      MetadataChangeProposal mcp =
          AspectUtils.buildMetadataChangeProposal(
              testUrn, AcrylConstants.BATCH_TEST_RUN_EVENT_ASPECT_NAME, event);
      mcp.setSystemMetadata(new SystemMetadata().setRunId(runId));
      entityClient.ingestProposal(systemOpContext, mcp);
    } catch (Exception e) {
      log.error(
          "Failed to produce Metadata Test Run Result aspect! This may mean that the results shown in the UI are stale!",
          e);
    }
  }
}
