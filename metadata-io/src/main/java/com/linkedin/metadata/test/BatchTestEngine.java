package com.linkedin.metadata.test;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.test.batch.BatchTestEngineEnvConfig;
import com.linkedin.metadata.test.batch.BatchTestEngineExecConfig;
import com.linkedin.metadata.test.batch.BatchTestResult;
import com.linkedin.metadata.test.batch.BatchTestResultAggregator;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.BatchTestRunStatus;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.PartitionSpec;
import com.linkedin.timeseries.PartitionType;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Used by the metadata tests cron-job for batch processing */
@Slf4j
public class BatchTestEngine {
  private final OperationContext systemOpContext;
  private final EntityClient entityClient;
  private final EntitySearchService entitySearchService;
  private final TestEngine testEngine;

  public BatchTestEngine(
      @Nonnull OperationContext systemOpContext,
      @Nonnull EntityClient entityClient,
      @Nonnull EntitySearchService entitySearchService,
      @Nonnull TestEngine testEngine) {
    this.systemOpContext = systemOpContext;
    this.entityClient = entityClient;
    this.entitySearchService = entitySearchService;
    this.testEngine = testEngine;
  }

  public void execute(BatchTestEngineEnvConfig envConfig, BatchTestEngineExecConfig execConfig) {
    execute(envConfig, execConfig, null);
  }

  @VisibleForTesting
  public void execute(
      BatchTestEngineEnvConfig envConfig,
      BatchTestEngineExecConfig execConfig,
      @Nullable ExecutorService overrideExecutorService) {
    Consumer<String> testLog = execConfig.getLogger();
    BatchTestResultAggregator resultAggregator = new BatchTestResultAggregator();

    ExecutorService executorService = null;
    try {
      executorService =
          overrideExecutorService != null
              ? overrideExecutorService
              : new ThreadPoolExecutor(
                  envConfig.getExecutorNumThreads(),
                  envConfig.getExecutorNumThreads(),
                  0L,
                  TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<>(envConfig.getExecutorQueueSize()),
                  new ThreadPoolExecutor.CallerRunsPolicy());

      Set<String> entityTypesToEvaluate = new HashSet<>(testEngine.getEntityTypesToEvaluate());

      testLog.accept(String.format("Evaluating tests for entities %s", entityTypesToEvaluate));

      List<Future<Map<Urn, TestResults>>> futures = new ArrayList<>();
      for (String entityType : entityTypesToEvaluate) {
        int batch = 1;
        String nextScrollId = null;
        do {
          testLog.accept(String.format("Fetching batch %d of %s entities", batch, entityType));
          ScrollResult scrollResult =
              entitySearchService.scroll(
                  systemOpContext,
                  Collections.singletonList(entityType),
                  null,
                  List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                  execConfig.getBatchSize(),
                  nextScrollId,
                  envConfig.getElasticsearchPitKeepAlive(),
                  null);
          nextScrollId = scrollResult.getScrollId();
          testLog.accept(String.format("Processing batch %d of %s entities", batch, entityType));
          Set<Urn> entitiesInBatch =
              scrollResult.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toSet());
          final int batchNumber = batch;
          futures.add(
              executorService.submit(
                  () ->
                      processBatch(
                          envConfig,
                          execConfig,
                          entitiesInBatch,
                          batchNumber,
                          entityType,
                          resultAggregator)));
          batch++;

          if (futures.size()
              >= Math.max(1, testEngine.getTestsConfiguration().getFuturesBatchSize())) {
            flushFutures(futures, testLog);
          }

          if (execConfig.getBatchDelayMs() > 0) {
            TimeUnit.MILLISECONDS.sleep(execConfig.getBatchDelayMs());
          }
        } while (nextScrollId != null);

        testLog.accept(
            String.format(
                "Finished submitting test evaluation for %s entities to worker pool.", entityType));
      }

      flushFutures(futures, testLog);

      testLog.accept("Finished evaluating tests for all entities");
      reportTestRunResults(
          resultAggregator.getTestResultSummaries().values(), execConfig.getRunId());

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (executorService != null) {
        TestEngine.shutdownExecutorService(executorService, "batch test executor", false);
      }
    }
  }

  private static void flushFutures(
      List<Future<Map<Urn, TestResults>>> futures, Consumer<String> testLog) {
    if (!futures.isEmpty()) {
      log.info("Flushing {} futures.", futures.size());
      for (Future<Map<Urn, TestResults>> results : futures) {
        // Wait for processing, we don't actually use the result currently for anything for now,
        // so treat as void
        try {
          results.get();
        } catch (InterruptedException | ExecutionException e) {
          testLog.accept("Reading interrupted, not able to finish processing.");
          throw new RuntimeException(e);
        }
      }
      futures.clear();
    }
  }

  private Map<Urn, TestResults> processBatch(
      BatchTestEngineEnvConfig envConfig,
      BatchTestEngineExecConfig execConfig,
      Set<Urn> entitiesInBatch,
      int batchNumber,
      String entityType,
      BatchTestResultAggregator resultAggregator)
      throws InterruptedException {
    {
      Map<Urn, TestResults> result;
      try {
        result =
            testEngine.evaluateTests(
                systemOpContext, entitiesInBatch, envConfig.getEvaluationMode());
        execConfig
            .getLogger()
            .accept(
                String.format(
                    "Pushed %d test results for batch %d of %s entities",
                    result.size(), batchNumber, entityType));
        incrementBatchCounters(result, resultAggregator);
        return result;
      } catch (Exception e) {
        execConfig
            .getLogger()
            .accept(
                String.format(
                    "Error while processing batch %d of %s entities", batchNumber, entityType));
        log.error("Error while processing batch {} of {} entities", batchNumber, entityType, e);
      } finally {
        if (execConfig.getBatchDelayMs() > 0) {
          TimeUnit.MILLISECONDS.sleep(execConfig.getBatchDelayMs());
        }
      }
      return null;
    }
  }

  /** Increment the counters used to report results for the metadata test */
  private static void incrementBatchCounters(
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
  private void reportTestRunResults(
      @Nonnull final Collection<BatchTestResult> results, String runId) {
    try {
      final long currentTime = System.currentTimeMillis();
      for (BatchTestResult result : results) {
        reportTestRunResult(result, currentTime, runId);
      }
    } catch (Exception e) {
      // Catch everything so that issuing in reporting never break all tests.
      log.error(
          "Caught exception while attempting to report Metadata Test run results! This may mean that one or more tests is missing results.",
          e);
    }
  }

  private void reportTestRunResult(
      @Nonnull final BatchTestResult result, final long currentTime, String runId) {
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
      entityClient.ingestProposal(systemOpContext, mcp, false);
    } catch (Exception e) {
      log.error(
          "Failed to produce Metadata Test Run Result aspect! This may mean that the results shown in the UI are stale!",
          e);
    }
  }
}
