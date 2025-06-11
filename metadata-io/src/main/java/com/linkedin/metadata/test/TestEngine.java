package com.linkedin.metadata.test;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.TestConstants.*;
import static com.linkedin.metadata.test.util.TestUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.patch.builder.TestResultsPatchBuilder;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.TestsHookExecutionLimitConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.TestAction;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.exception.SelectionTooLargeException;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import com.linkedin.metadata.test.executor.elastic.ElasticSearchTestExecutor;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.test.util.TestUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.BatchTestRunResult;
import com.linkedin.test.BatchTestRunStatus;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestInterval;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestSourceType;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Main engine responsible for evaluating all tests for all entities or for a given entity
 *
 * <p>Currently, the engine is implemented as a spring-instantiated Singleton which manages its own
 * thread-pool used for resolving tests.
 */
@Slf4j
public class TestEngine implements AutoCloseable {
  private final EntityService<?> entityService;

  private final EntitySearchService searchService;

  private final QueryEngine queryEngine;
  private final PredicateEvaluator predicateEvaluator;
  private final TestDefinitionParser testDefinitionParser;
  private final ActionApplier actionApplier;

  // Maps test urn to deserialized test definition
  // Not concurrent data structure because writes are always against the entire
  // thing.
  // _testCache does not contain tests without a schedule. These tests should only
  // be run manually and do not get picked up in scheduled runs.
  private final Map<Urn, TestDefinition> testCache = new HashMap<>();
  private final Map<Urn, TestInfo> testInfoCache = new HashMap<>();
  // Maps entity type to list of tests that target the entity type
  // Not concurrent data structure because writes are always against the entire
  // thing.
  private final Map<String, Set<TestDefinition>> testPerEntityTypeCache = new HashMap<>();

  // Shared lock for both cache hashmaps
  private final ReadWriteLock cacheReadWriteLock = new ReentrantReadWriteLock();
  private final Lock cacheReadLock = cacheReadWriteLock.readLock();

  private ScheduledExecutorService refreshExecutorService;
  private final TestRefreshRunnable testRefreshRunnable;
  private final Set<String> supportedEntityTypes;
  private final boolean isPartialFetcher;

  @Getter private final ElasticSearchTestExecutor elasticSearchTestExecutor;

  private final TimeseriesAspectService timeseriesAspectService;

  private final OperationContext systemOpContext;

  private final boolean elasticSearchExecutorEnabled;
  private final TestsHookExecutionLimitConfiguration executionLimits;
  @VisibleForTesting @Getter final ExecutorService actionsExecutorService;
  @Getter private final TestsConfiguration testsConfiguration;

  /** The test engine evaluation mode. */
  public enum EvaluationMode {
    /**
     * A transient evaluation, where Test Results are not stored anywhere, actions are not applied,
     * and results are simply returned to the client.
     *
     * <p>This is useful for trying out a Metadata Test definition before it's configured
     * completely.
     */
    EVALUATE_ONLY,

    /**
     * Default evaluation mode, where results are saved to storage async and actions are applied
     * async.
     */
    DEFAULT,

    /** Results are saved to storage sync and actions are applied sync. */
    SYNC,

    /** Results are saved to storage sync and actions are applied async. */
    SYNC_WITH_ACTIONS_ASYNC;

    public static EvaluationMode getEvaluationMode(String mode) {
      try {
        return EvaluationMode.valueOf(mode);
      } catch (Exception e) {
        log.info("Unable to resolve evaluation mode {}, using default.", mode);
        return EvaluationMode.DEFAULT;
      }
    }
  }

  public TestEngine(
      @Nonnull OperationContext systemOpContext,
      TestsConfiguration testsConfiguration,
      EntityService<?> entityService,
      EntitySearchService searchService,
      TimeseriesAspectService timeseriesAspectService,
      TestFetcher testFetcher,
      TestDefinitionParser testDefinitionParser,
      QueryEngine queryEngine,
      PredicateEvaluator predicateEvaluator,
      ActionApplier actionApplier,
      @Nullable ExecutorService actionsExecutorService) {

    this.testsConfiguration = testsConfiguration;
    this.entityService = entityService;
    this.searchService = searchService;
    this.queryEngine = queryEngine;
    this.predicateEvaluator = predicateEvaluator;
    this.testDefinitionParser = testDefinitionParser;
    this.actionApplier = actionApplier;
    isPartialFetcher = testFetcher.isPartial();
    this.systemOpContext = systemOpContext;
    testRefreshRunnable =
        new TestRefreshRunnable(
            systemOpContext,
            testFetcher,
            testDefinitionParser,
            testCache,
            testInfoCache,
            testPerEntityTypeCache,
            cacheReadWriteLock.writeLock());

    if (testsConfiguration.isEnabled() || testsConfiguration.getHook().isEnabled()) {
      if (testsConfiguration.getCacheRefreshIntervalSecs() > 0) {
        refreshExecutorService = Executors.newScheduledThreadPool(1);
        refreshExecutorService.scheduleAtFixedRate(
            testRefreshRunnable,
            testsConfiguration.getCacheRefreshDelayIntervalSecs(),
            testsConfiguration.getCacheRefreshIntervalSecs(),
            TimeUnit.SECONDS);
        refreshExecutorService.execute(testRefreshRunnable);
      } else {
        loadTests();
      }
    } else {
      log.info("Metadata tests is disabled, not fetching test definitions");
    }

    supportedEntityTypes = TestUtils.getSupportedEntityTypes(systemOpContext.getEntityRegistry());
    elasticSearchTestExecutor =
        new ElasticSearchTestExecutor(
            searchService,
            timeseriesAspectService,
            systemOpContext.getEntityRegistry(),
            systemOpContext,
            testsConfiguration.getHook().getHookExecutionLimit().getElasticSearchExecutor());
    this.timeseriesAspectService = timeseriesAspectService;
    this.elasticSearchExecutorEnabled = testsConfiguration.getElasticSearchExecutor().isEnabled();
    this.executionLimits = testsConfiguration.getHook().getHookExecutionLimit();
    this.actionsExecutorService = actionsExecutorService;

    if (testsConfiguration.isJvmShutdownHookEnabled()) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    log.info(
                        "Metadata Tests - JVM shutdown hook triggered - initiating graceful shutdown");
                    close();
                  }));
    }
  }

  /**
   * Cleanly shuts down all executor services, ensuring all pending tasks complete or timeout.
   * Should be called when TestEngine is being destroyed.
   */
  @Override
  public void close() {
    shutdownExecutorService(refreshExecutorService, "refresh executor", false);
    shutdownExecutorService(actionsExecutorService, "actions executor", false);
  }

  public void forceClose() {
    shutdownExecutorService(refreshExecutorService, "refresh executor", true);
    shutdownExecutorService(actionsExecutorService, "actions executor", true);
  }

  /**
   * Shuts down an executor service, waiting indefinitely for tasks to complete unless force
   * shutdown is triggered.
   */
  static void shutdownExecutorService(
      @Nullable ExecutorService executor, String executorName, boolean forceShutdown) {
    if (executor == null || executor.isShutdown()) {
      return;
    }

    try {
      log.info("Initiating shutdown of {} service...", executorName);

      // First reject new tasks
      executor.shutdown();

      int pendingTasks =
          executor instanceof ThreadPoolExecutor
              ? ((ThreadPoolExecutor) executor).getQueue().size()
              : 0;

      log.info(
          "Waiting for {} pending tasks to complete in {} service", pendingTasks, executorName);

      // Wait indefinitely in a loop, checking for force shutdown
      while (!executor.isTerminated()) {
        if (forceShutdown) {
          log.warn("Force shutdown triggered for {} service", executorName);
          executor.shutdownNow();
          break;
        }

        // Wait in smaller intervals to be responsive to force shutdown
        boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        if (!terminated && executor instanceof ThreadPoolExecutor) {
          // Log progress periodically
          int remaining = ((ThreadPoolExecutor) executor).getQueue().size();
          if (remaining > 0) {
            log.info("{} tasks still pending in {} service", remaining, executorName);
          }
        }
      }

      if (executor.isTerminated()) {
        log.info("Successfully shut down {} service", executorName);
      } else {
        log.warn("{} service shutdown incomplete - some tasks may not have finished", executorName);
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while shutting down {} service", executorName, e);
      executor.shutdownNow();
    } catch (Exception e) {
      log.error("Error during shutdown of {} service", executorName, e);
      executor.shutdownNow();
    }
  }

  /**
   * Invalidates the test cache and fires off a refresh thread. Should be invoked when a test is
   * created, modified, or deleted.
   */
  public void invalidateCache() {
    if (refreshExecutorService != null) {
      refreshExecutorService.execute(testRefreshRunnable);
    }
  }

  /** Synchronously refresh the Metadata Tests cache. */
  public void loadTests() {
    testRefreshRunnable.run();
  }

  @Nonnull
  public Set<String> getEntityTypesToEvaluate() {
    cacheReadLock.lock();
    try {
      return testPerEntityTypeCache.keySet();
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
  }

  @Nonnull
  public TestDefinitionParser getParser() {
    return testDefinitionParser;
  }

  /**
   * Validates a JSON-serialized test definition using a {@link TestDefinitionParser}
   *
   * @param definitionJson the test definition, serialized as json.
   * @return a {@link ValidationResult}
   */
  @Nonnull
  public ValidationResult validateJson(@Nonnull final String definitionJson) {
    // Try to deserialize json definition
    TestDefinition testDefinition;
    try {
      testDefinition = testDefinitionParser.deserialize(DUMMY_TEST_URN, definitionJson);
    } catch (TestDefinitionParsingException | IllegalArgumentException e) {
      return new ValidationResult(false, Collections.singletonList(e.getMessage()));
    }
    return validateTestDefinition(testDefinition);
  }

  /**
   * Evaluate all eligible tests for the given urn, optionally write the results to GMS. If no
   * eligible tests are found for the urn, an empty set of test results will be returned.
   *
   * @param urn Entity urn to evaluate
   * @param mode The evaluation mode.
   * @return Test results
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  @Nonnull
  public TestResults evaluateTests(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn, @Nonnull EvaluationMode mode) {
    return evaluateTests(opContext, Set.of(urn), Set.of(), mode).getOrDefault(urn, EMPTY_RESULTS);
  }

  /**
   * Evaluate input tests for the given urn, then optionally write results to GMS. If no eligible
   * tests are found for the urn, an empty set of test results will be returned.
   *
   * @param urn Entity urn to evaluate
   * @param testUrns Tests to evaluate
   * @param mode Whether to push the test results into DataHub or not
   * @return Test results.
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  @Nonnull
  public TestResults evaluateTestUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull Set<Urn> testUrns,
      @Nonnull EvaluationMode mode) {
    return evaluateTestUrns(opContext, Set.of(urn), testUrns, mode)
        .getOrDefault(urn, EMPTY_RESULTS);
  }

  /**
   * Batch evaluate all eligible tests for input set of urns.
   *
   * @param urns Entity urns to evaluate
   * @param mode The evaluation mode
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> evaluateTests(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final EvaluationMode mode) {
    return evaluateTests(opContext, urns, Set.of(), mode);
  }

  public Map<Urn, TestResults> evaluateTestUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> entityUrns,
      @Nonnull final Set<Urn> testUrns,
      @Nonnull EvaluationMode mode) {
    return evaluateTests(
        opContext, entityUrns, testUrns.isEmpty() ? Set.of() : fetchDefinitions(testUrns), mode);
  }

  /**
   * Evaluate tests, optionally write the results to GMS. If no eligible tests are found for the
   * urn, an empty set of test results will be returned.
   *
   * @param entityUrns If non-empty, evaluate tests
   * @param tests If non-empty, evaluate the provided tests. If empty, all tests for entities.
   * @param mode The evaluation mode.
   * @return Test Results map
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  public Map<Urn, TestResults> evaluateTests(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> entityUrns,
      @Nonnull final Set<TestDefinition> tests,
      @Nonnull EvaluationMode mode) {
    if (entityUrns.isEmpty()) {
      return Map.of();
    }

    final Set<String> entityTypes =
        entityUrns.stream().map(Urn::getEntityType).collect(Collectors.toSet());

    // Step 0: Check for supported entities
    for (String entityType : entityTypes) {
      if (!supportedEntityTypes.contains(entityType)) {
        log.warn(
            String.format(
                "Attempted to evaluate tests for an unsupported entity type %s.", entityType));
        throw new UnsupportedOperationException(
            String.format(
                "Attempted to evaluate tests for an unsupported entity type %s. Returning null results.",
                entityType));
      }
    }

    // Step 1: Retrieve eligible tests.
    final Set<TestDefinition> testDefinitions;
    final boolean partial;
    if (tests.isEmpty()) {
      partial = isPartialFetcher;
      testDefinitions =
          entityTypes.stream()
              .flatMap(entityType -> getOrDefault(entityType, Set.of()).stream())
              .collect(Collectors.toSet());
    } else {
      partial = true;
      testDefinitions =
          tests.stream()
              .flatMap(
                  testDef ->
                      testDef.getOn().getEntityTypes().stream()
                          .map(entryType -> Map.entry(entryType, testDef)))
              .filter(entityTestDef -> entityTypes.contains(entityTestDef.getKey()))
              .map(Map.Entry::getValue)
              .collect(Collectors.toSet());
    }

    HashMap<Urn, BatchTestRunResult> batchedTestRunResults = new HashMap<>();
    // Step 2: Evaluate all tests for entity
    Map<Urn, TestResults> results =
        batchEvaluateTests(entityUrns, testDefinitions, batchedTestRunResults);

    // Step 3 (Optional): Write results to DataHub
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      batchIngestResults(results, mode, partial, null, true);
      batchApplyActions(opContext, results, null, mode);
    }

    return results;
  }

  /**
   * Evaluates tests against a set of entity urns in batch.
   *
   * @param urns the urns to be tested
   * @param tests the test definitions to be evaluated
   * @return a map of entity urn to the corresponding test results.
   */
  @WithSpan
  private Map<Urn, TestResults> batchEvaluateTests(
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<TestDefinition> tests,
      @Nonnull Map<Urn, BatchTestRunResult> batchTestRunResults) {
    // Get a map of test definition -> urns which match select conditions
    Map<TestDefinition, Set<Urn>> eligibleTestsPerEntity = getEligibleEntitiesPerTest(urns, tests);

    Map<Urn, TestResults> finalTestResults =
        urns.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    urn ->
                        new TestResults()
                            .setPassing(new TestResultArray())
                            .setFailing(new TestResultArray())));

    // For each test with eligible entities
    for (TestDefinition testDefinition : eligibleTestsPerEntity.keySet()) {
      Set<Urn> urnsToTest = eligibleTestsPerEntity.get(testDefinition);

      try {
        // Batch evaluate all queries in the main rules for all eligible entities
        Map<Urn, Map<TestQuery, TestQueryResponse>> rulesQueryResponses =
            batchQuery(urnsToTest, ImmutableList.of(testDefinition.getRules()));

        if (!batchTestRunResults.containsKey(testDefinition.getUrn())) {
          batchTestRunResults.put(
              testDefinition.getUrn(),
              new BatchTestRunResult()
                  .setPassingCount(0)
                  .setFailingCount(0)
                  .setTestDefinition(testDefinition.getRawDefinition()));
        }

        // For each entity, evaluate whether it passes the test using the query
        // evaluation results
        for (Urn urn : urnsToTest) {

          // Run the test!
          final boolean isUrnPassingTest =
              predicateEvaluator.evaluatePredicate(
                  testDefinition.getRules(),
                  rulesQueryResponses.getOrDefault(urn, Collections.emptyMap()));

          if (isUrnPassingTest) {
            finalTestResults
                .get(urn)
                .getPassing()
                .add(
                    new TestResult()
                        .setTest(testDefinition.getUrn())
                        .setType(TestResultType.SUCCESS)
                        .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL)
                        .setLastComputed(
                            new AuditStamp()
                                .setTime(System.currentTimeMillis())
                                .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))));
            batchTestRunResults
                .get(testDefinition.getUrn())
                .setPassingCount(
                    batchTestRunResults.get(testDefinition.getUrn()).getPassingCount() + 1);
          } else {
            finalTestResults
                .get(urn)
                .getFailing()
                .add(
                    new TestResult()
                        .setTest(testDefinition.getUrn())
                        .setType(TestResultType.FAILURE)
                        .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL)
                        .setLastComputed(
                            new AuditStamp()
                                .setTime(System.currentTimeMillis())
                                .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))));
            batchTestRunResults
                .get(testDefinition.getUrn())
                .setFailingCount(
                    batchTestRunResults.get(testDefinition.getUrn()).getFailingCount() + 1);
          }
        }
      } catch (RuntimeException e) {
        log.error(
            "Error in evaluating Test: {} Definition: {}",
            testDefinition.getUrn(),
            testDefinition.getRawDefinition(),
            e);
      }
    }
    return finalTestResults;
  }

  /**
   * For each test, return the list of eligible entities based on matching against the select
   * conditions.
   *
   * @param urns the entity urns
   * @param tests the list of test definitions
   * @return a map of the test definition to the URNs which should be evaluated.
   */
  private Map<TestDefinition, Set<Urn>> getEligibleEntitiesPerTest(
      @Nonnull Set<Urn> urns, @Nonnull Set<TestDefinition> tests) {
    // First batch evaluate all queries in the targeting rules
    // Always make sure we batch queries together as much as possible to reduce
    // calls
    Map<Urn, Map<TestQuery, TestQueryResponse>> selectConditionQueryResponse =
        batchQuery(urns, getPredicatesFromSelectConditions(tests));
    Map<TestDefinition, Set<Urn>> eligibleTestsPerEntity = new HashMap<>();

    // Using the query evaluation result, find the set of eligible tests per entity.
    // Reverse map to get the list of entities eligible for each test
    for (Urn urn : urns) {
      Map<TestQuery, TestQueryResponse> queryResponseForUrn =
          selectConditionQueryResponse.getOrDefault(urn, Collections.emptyMap());
      tests.stream()
          .filter(test -> evaluateSelectConditions(urn, queryResponseForUrn, test))
          .forEach(
              test -> {
                if (!eligibleTestsPerEntity.containsKey(test)) {
                  eligibleTestsPerEntity.put(test, new HashSet<>());
                }
                eligibleTestsPerEntity.get(test).add(urn);
              });
    }
    return eligibleTestsPerEntity;
  }

  /**
   * Validate all query operators present in a {@link TestDefinition}.
   *
   * @param testDefinition the test definition itself
   * @return a {@link ValidationResult} detailing whether validation passed or failed.
   */
  private ValidationResult validateTestDefinition(@Nonnull final TestDefinition testDefinition) {
    List<String> entityTypes = testDefinition.getOn().getEntityTypes();
    List<TestQuery> queries = new ArrayList<>();

    // Validate 'on' block queries.
    if (testDefinition.getOn().getConditions() != null) {
      queries.addAll(Predicate.extractQueriesForPredicate(testDefinition.getOn().getConditions()));
    }

    // Validate 'rules' block queries.
    queries.addAll(Predicate.extractQueriesForPredicate(testDefinition.getRules()));

    // Verify that each defined query is valid. If multiple are invalid, merge the
    // error messages
    List<ValidationResult> invalidResults =
        queries.stream()
            .map(query -> queryEngine.validateQuery(query, entityTypes))
            .filter(result -> !result.isValid())
            .collect(Collectors.toList());

    if (invalidResults.isEmpty()) {
      return ValidationResult.validResult();
    }
    return new ValidationResult(
        false,
        invalidResults.stream()
            .flatMap(result -> result.getMessages().stream())
            .collect(Collectors.toList()));
  }

  @WithSpan
  private Map<Urn, Map<TestQuery, TestQueryResponse>> batchQuery(
      @Nonnull final Set<Urn> urns, @Nonnull final List<Predicate> rules) {
    if (rules.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<TestQuery> requiredQueries =
        rules.stream()
            .flatMap(rule -> Predicate.extractQueriesForPredicate(rule).stream())
            .collect(Collectors.toSet());
    return queryEngine.batchEvaluateQueries(systemOpContext, new HashSet<>(urns), requiredQueries);
  }

  private List<Predicate> getPredicatesFromSelectConditions(
      @Nonnull final Set<TestDefinition> tests) {
    return tests.stream()
        .map(test -> test.getOn().getConditions())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Set<TestDefinition> fetchDefinitions(@Nonnull final Set<Urn> testUrns) {
    Set<TestDefinition> tests = new HashSet<>(testUrns.size());

    cacheReadLock.lock();
    try {
      for (Urn testUrn : testUrns) {
        if (testCache.containsKey(testUrn)) {
          tests.add(testCache.get(testUrn));
        } else {
          log.warn("Test {} does not exist: Skipping", testUrn);
        }
      }
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }

    return tests;
  }

  /**
   * Evaluate whether the entity passes the select rules in the test. Note, we batch evaluate
   * queries before calling this function, and the results are inputted as arguments This function
   * purely uses the query evaluation results and checks whether it passes the targeting rules or
   * not
   *
   * @param urn Entity urn in question
   * @param targetingRulesQueryResponses Batch query evaluation results. Contains the result of all
   *     queries in the targeting rules
   * @param test Test that we are trying to evaluate
   * @return Whether or not the entity passes the targeting rules of the test
   */
  private boolean evaluateSelectConditions(
      Urn urn,
      Map<TestQuery, TestQueryResponse> targetingRulesQueryResponses,
      TestDefinition test) {

    // If the URN is not eligible, return false.
    if (!test.getOn().getEntityTypes().contains(urn.getEntityType())) {
      return false;
    }
    Predicate selectConditions = test.getOn().getConditions();

    // If there are no conditions, simply return true!
    if (selectConditions == null) {
      return true;
    }
    return predicateEvaluator.evaluatePredicate(selectConditions, targetingRulesQueryResponses);
  }

  private TestResultArray computeUpdated(
      TestResultArray previousResults, TestResultArray newResults) {
    TestResultArray updatedResults = new TestResultArray();
    for (TestResult newResult : newResults) {
      if (previousResults.stream()
          .noneMatch(
              r ->
                  (r.getTest().equals(newResult.getTest())
                      && Objects.equals(
                          r.getTestDefinitionMd5(), newResult.getTestDefinitionMd5())))) {
        updatedResults.add(newResult);
      }
    }
    return updatedResults;
  }

  /**
   * Overwrites test results into GMS for a given urn. This function is designed to operate on a
   * subset of target entities for the test.
   *
   * @param testResults Test result map
   * @param mode Evaluation mode
   */
  private void batchIngestResults(
      @Nonnull Map<Urn, TestResults> testResults,
      EvaluationMode mode,
      boolean partial,
      @Nullable Map<Urn, TestResults> previousResults,
      boolean shouldWriteAssetResults) {
    Stream<MetadataChangeProposal> mcps = Stream.empty();
    Stream<MetadataChangeProposal> removalMcps = Stream.empty();

    if (shouldWriteAssetResults) {
      if (partial) {
        if (previousResults != null) {
          // For the entities that were ONLY present in the previous results, we need to remove
          // the test results. For entities that are common, the test result update will
          // automatically
          // remove
          // the old test results.
          Set<Urn> onlyInPrevious =
              previousResults.keySet().stream()
                  .filter(urn -> !testResults.containsKey(urn))
                  .collect(Collectors.toSet());
          log.info("Only in previous: {}", onlyInPrevious);
          removalMcps =
              onlyInPrevious.stream()
                  .map(
                      urn -> {
                        final TestResultsPatchBuilder builder =
                            new TestResultsPatchBuilder().urn(urn);
                        previousResults
                            .get(urn)
                            .getPassing()
                            .forEach(
                                testResult ->
                                    builder.removePassing(testResult.getTest(), testResult));
                        previousResults
                            .get(urn)
                            .getFailing()
                            .forEach(
                                testResult ->
                                    builder.removeFailing(testResult.getTest(), testResult));
                        return builder.build();
                      });
        }

        mcps =
            testResults.entrySet().stream()
                .map(
                    entry -> {
                      final TestResultsPatchBuilder builder =
                          new TestResultsPatchBuilder().urn(entry.getKey());
                      if (previousResults != null && previousResults.containsKey(entry.getKey())) {
                        TestResults finalTestResults =
                            new TestResults()
                                .setPassing(
                                    computeUpdated(
                                        previousResults.get(entry.getKey()).getPassing(),
                                        entry.getValue().getPassing()))
                                .setFailing(
                                    computeUpdated(
                                        previousResults.get(entry.getKey()).getFailing(),
                                        entry.getValue().getFailing()));
                        builder.updateTestResults(finalTestResults);
                      } else {
                        builder.updateTestResults(entry.getValue());
                      }
                      return builder;
                    })
                .filter(TestResultsPatchBuilder::hasPatchOperations)
                .map(TestResultsPatchBuilder::build);
      } else {
        mcps =
            testResults.entrySet().stream()
                .map(
                    entry -> {
                      final MetadataChangeProposal proposal = new MetadataChangeProposal();
                      proposal.setEntityUrn(entry.getKey());
                      proposal.setEntityType(entry.getKey().getEntityType());
                      proposal.setAspectName(Constants.TEST_RESULTS_ASPECT_NAME);
                      proposal.setAspect(GenericRecordUtils.serializeAspect(entry.getValue()));
                      proposal.setChangeType(ChangeType.UPSERT);
                      return proposal;
                    });
      }
    }

    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    List<MetadataChangeProposal> allMCPs =
        Stream.concat(mcps, removalMcps).collect(Collectors.toList());
    log.info("Total number of mcps = {}", allMCPs.size());
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcps(allMCPs, auditStamp, systemOpContext.getRetrieverContext())
            .build();

    entityService.ingestProposal(systemOpContext, batch, mode != EvaluationMode.SYNC);
  }

  /**
   * Batch applies actions for an set of entities.
   *
   * <p>To do this, we first group by the Action -> entity URNs requiring the action. Then, we batch
   * apply the action to all entity URNs in the set using an {@link ActionApplier}.
   */
  private void batchApplyActions(
      @Nonnull OperationContext opContext,
      @Nonnull final Map<Urn, TestResults> entityUrnToResults,
      @Nullable final TestDefinition testDefinition,
      @Nonnull EvaluationMode mode) {
    // First, aggregate by Action -> URNs, so we can batch the action execution
    // itself.
    final Map<TestAction, Set<Urn>> actionToEntityUrns = new HashMap<>();
    for (Map.Entry<Urn, TestResults> entry : entityUrnToResults.entrySet()) {
      addActionsToEntityMap(
          entry.getKey(),
          entry.getValue().getPassing(),
          actionToEntityUrns,
          testDefinition); // Passing Actions
      addActionsToEntityMap(
          entry.getKey(),
          entry.getValue().getFailing(),
          actionToEntityUrns,
          testDefinition); // Failing Actions
    }

    if (EvaluationMode.SYNC.equals(mode) || actionsExecutorService == null) {
      if (actionsExecutorService == null && !EvaluationMode.SYNC.equals(mode)) {
        log.warn("Async actions requested, however actions executor service is null.");
      }
      applyActions(opContext, actionToEntityUrns);
    } else {
      // Submit to executor service with error handling
      CompletableFuture.runAsync(
          () -> {
            try {
              applyActions(opContext, actionToEntityUrns);
            } catch (Exception e) {
              log.error("Failed to execute metadata tests batch actions", e);
            }
          },
          actionsExecutorService);
    }
  }

  private void applyActions(
      @Nonnull OperationContext opContext, Map<TestAction, Set<Urn>> actionToEntityUrns) {
    for (Map.Entry<TestAction, Set<Urn>> entry : actionToEntityUrns.entrySet()) {
      try {
        TestAction action = entry.getKey();
        List<Urn> urns = new ArrayList<>(entry.getValue());

        actionApplier.apply(
            opContext, action.getType(), urns, new ActionParameters(action.getParams()));
      } catch (Exception e) {
        log.error("Failed to apply metadata test action: {}", entry.getKey(), e);
      }
    }
  }

  /** Batch applies actions for an entity. */
  private void addActionsToEntityMap(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<TestResult> testResults,
      @Nonnull final Map<TestAction, Set<Urn>> actionToEntityUrns,
      @Nullable final TestDefinition testDefinition) {

    cacheReadLock.lock();
    try {
      for (TestResult result : testResults) {
        TestDefinition definition =
            testDefinition != null ? testDefinition : testCache.get(result.getTest());
        if (definition == null) {
          log.warn("Test {} does not exist: Skipping", result.getTest());
          continue;
        }
        List<TestAction> eligibleActions =
            TestResultType.FAILURE.equals(result.getType())
                ? definition.getActions().getFailing()
                : definition.getActions().getPassing();
        for (TestAction action : eligibleActions) {
          actionToEntityUrns.putIfAbsent(action, new HashSet<>());
          actionToEntityUrns.get(action).add(entityUrn);
        }
      }
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
  }

  private Set<TestDefinition> getOrDefault(String key, Set<TestDefinition> defaultValue) {
    cacheReadLock.lock();
    try {
      return testPerEntityTypeCache.getOrDefault(key, defaultValue);
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
  }

  private Optional<BatchTestRunEvent> getLastExecution(Urn testUrn) {
    List<EnvelopedAspect> lastComputed =
        this.timeseriesAspectService.getAspectValues(
            systemOpContext,
            testUrn,
            TEST_ENTITY_NAME,
            BATCH_TEST_RUN_EVENT_ASPECT_NAME,
            null,
            null,
            1,
            null);
    if (!lastComputed.isEmpty()) {
      EnvelopedAspect envelopedAspect = lastComputed.get(0);
      BatchTestRunEvent batchTestRunEvent =
          GenericRecordUtils.deserializeAspect(
              envelopedAspect.getAspect().getValue(), "application/json", BatchTestRunEvent.class);
      return Optional.of(batchTestRunEvent);
    } else {
      return Optional.empty();
    }
  }

  private Map<Urn, TestResults> getMaterializedResults(
      Urn testUrn, TestDefinition testDefinition, @Nonnull AspectRetriever aspectRetriever) {
    Filter passingFilter =
        TestUtils.buildTestPassingFilter(testUrn, testDefinition.getMd5(), aspectRetriever);
    Filter failingFilter =
        TestUtils.buildTestFailingFilter(testUrn, testDefinition.getMd5(), aspectRetriever);
    Map<Urn, TestResults> results = new HashMap<>();
    ScrollResult scrollResult =
        this.searchService.scroll(
            systemOpContext,
            testDefinition.getOn().getEntityTypes(),
            passingFilter,
            null,
            1000,
            null,
            "1m",
            null);
    scrollResult
        .getEntities()
        .forEach(
            (entity) -> {
              log.info("Old Passing entity: {} ", entity.getEntity());
              Urn urn = entity.getEntity();
              TestResults testResults =
                  new TestResults()
                      .setPassing(new TestResultArray())
                      .setFailing(new TestResultArray());
              testResults
                  .getPassing()
                  .add(
                      new TestResult()
                          .setTest(testUrn)
                          .setType(TestResultType.SUCCESS)
                          .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL));
              results.put(urn, testResults);
            });
    while (scrollResult.getNumEntities() == 1000) {
      String scrollId = scrollResult.getScrollId();
      scrollResult =
          this.searchService.scroll(
              systemOpContext,
              testDefinition.getOn().getEntityTypes(),
              passingFilter,
              null,
              1000,
              scrollId,
              "1m",
              null);
      scrollResult
          .getEntities()
          .forEach(
              (entity) -> {
                log.info("Old Passing entity: {} ", entity.getEntity());
                Urn urn = entity.getEntity();
                TestResults testResults =
                    new TestResults()
                        .setPassing(new TestResultArray())
                        .setFailing(new TestResultArray());
                testResults
                    .getPassing()
                    .add(
                        new TestResult()
                            .setTest(testUrn)
                            .setType(TestResultType.SUCCESS)
                            .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL));
                results.put(urn, testResults);
              });
    }
    scrollResult =
        this.searchService.scroll(
            systemOpContext,
            testDefinition.getOn().getEntityTypes(),
            failingFilter,
            null,
            1000,
            null,
            "1m",
            null);
    scrollResult
        .getEntities()
        .forEach(
            (entity) -> {
              log.info("Old Failing entity: {} ", entity.getEntity());
              Urn urn = entity.getEntity();
              TestResults testResults =
                  new TestResults()
                      .setPassing(new TestResultArray())
                      .setFailing(new TestResultArray());
              testResults
                  .getFailing()
                  .add(
                      new TestResult()
                          .setTest(testUrn)
                          .setType(TestResultType.FAILURE)
                          .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL));
              results.put(urn, testResults);
            });
    while (scrollResult.getNumEntities() == 1000) {
      String scrollId = scrollResult.getScrollId();
      scrollResult =
          this.searchService.scroll(
              systemOpContext,
              testDefinition.getOn().getEntityTypes(),
              failingFilter,
              null,
              1000,
              scrollId,
              "1m",
              null);
      scrollResult
          .getEntities()
          .forEach(
              (entity) -> {
                log.info("Old Failing entity: {} ", entity.getEntity());
                Urn urn = entity.getEntity();
                TestResults testResults =
                    new TestResults()
                        .setPassing(new TestResultArray())
                        .setFailing(new TestResultArray());
                testResults
                    .getFailing()
                    .add(
                        new TestResult()
                            .setTest(testUrn)
                            .setType(TestResultType.FAILURE)
                            .setTestDefinitionMd5(testDefinition.getMd5(), SetMode.IGNORE_NULL));
                results.put(urn, testResults);
              });
    }
    return results;
  }

  /**
   * Evaluates a single test
   *
   * @param testUrn test entity to evaluate
   * @param mode whether to run actions
   */
  public Map<Urn, TestResults> evaluateSingleTest(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn testUrn,
      @Nonnull EvaluationMode mode) {
    log.info("Evaluating single test {} with mode {}", testUrn, mode);
    this.refreshSingleTest(testUrn);
    TestDefinition testDefinition;
    TestInfo testInfo;
    cacheReadLock.lock();
    try {
      testDefinition = testCache.get(testUrn);
      testInfo = testInfoCache.get(testUrn);
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
    // when evaluating a single test, try fetching the test from db if not in cache
    // tests without a schedule do not exist in the cache
    if (testDefinition == null || testInfo == null) {
      testInfo = getTestInfo(opContext, testUrn);
      if (testInfo == null) {
        log.warn("Test {} does not exist: Skipping", testUrn);
        return null;
      }
      try {
        testDefinition =
            testDefinitionParser.deserialize(testUrn, testInfo.getDefinition().getJson());
        ValidationResult validationResult = validateTestDefinition(testDefinition);
        if (!validationResult.isValid()) {
          throw new TestDefinitionParsingException(
              "Test definition " + testUrn + " is invalid: " + validationResult.getMessages());
        }
      } catch (TestDefinitionParsingException | IllegalArgumentException e) {
        log.error(
            "Issue while deserializing test definition {}", testInfo.getDefinition().getJson(), e);
        return null;
      }
    }

    // First retrieve the last execution of this test
    final Optional<BatchTestRunEvent> maybeLastExecution = getLastExecution(testUrn);
    Map<Urn, TestResults> oldResults = null;
    if (maybeLastExecution.isPresent() && shouldWriteAssetResults(testInfo)) {
      BatchTestRunEvent lastExecution = maybeLastExecution.get();
      log.info("Last execution of test {} found: {}", testUrn, lastExecution);
      if (lastExecution.getResult() != null
          && lastExecution.getResult().getTestDefinition() != null) {
        TestDefinition oldTestDefinition =
            this.testDefinitionParser.deserialize(
                testUrn, lastExecution.getResult().getTestDefinition());

        log.info("Fetching old results");
        oldResults =
            getMaterializedResults(testUrn, oldTestDefinition, opContext.getAspectRetriever());
        log.info("Old results size for test {} = {}", testUrn, oldResults.size());
        oldResults.forEach(
            (urn, testResults) -> {
              log.info("Entity {} results: {}", urn, testResults);
            });
      }
    } else {
      log.info("No last execution of test {} found", testUrn);
    }

    log.info("Evaluating single test {} with mode {}", testUrn, mode);

    final Map<Urn, BatchTestRunResult> batchTestRunResults = new HashMap<>();
    batchTestRunResults.put(
        testUrn,
        new BatchTestRunResult()
            .setPassingCount(0)
            .setFailingCount(0)
            .setTestDefinition(testDefinition.getRawDefinition()));
    log.info("BatchTestRunResult pre eval: {}", batchTestRunResults.get(testUrn));

    final Map<Urn, TestResults> results =
        getUrnTestResultsMap(testUrn, testDefinition, batchTestRunResults, testInfo);
    log.info("BatchTestRunResult post eval: {}", batchTestRunResults.get(testUrn));
    // (Optional): Write results to DataHub
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      log.info(
          "Mode: {}: Writing {} results to DataHub for test {}",
          mode,
          results.keySet().size(),
          testUrn);

      // Writes entity results
      // skip writing asset results if the source type is bulk form submission or form assignment
      batchIngestResults(results, mode, true, oldResults, shouldWriteAssetResults(testInfo));

      // Writes test summary results
      List<MetadataChangeProposal> reportingMCPs =
          batchTestRunResults.entrySet().stream()
              .map(
                  entry -> {
                    final MetadataChangeProposal proposal = new MetadataChangeProposal();
                    proposal.setEntityUrn(entry.getKey());
                    proposal.setEntityType(entry.getKey().getEntityType());
                    proposal.setAspectName(BATCH_TEST_RUN_EVENT_ASPECT_NAME);
                    proposal.setAspect(
                        GenericRecordUtils.serializeAspect(
                            new BatchTestRunEvent()
                                .setTimestampMillis(System.currentTimeMillis())
                                .setStatus(BatchTestRunStatus.COMPLETE)
                                .setResult(entry.getValue())));
                    proposal.setChangeType(ChangeType.UPSERT);
                    proposal.setSystemMetadata(
                        new SystemMetadata().setRunId(String.format("single-%s", Instant.now())));
                    log.info("Reporting Batch Test Result: {}", entry.getValue());
                    return proposal;
                  })
              .collect(Collectors.toList());
      AspectsBatchImpl batch =
          AspectsBatchImpl.builder()
              .mcps(
                  reportingMCPs,
                  new AuditStamp()
                      .setTime(System.currentTimeMillis())
                      .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)),
                  systemOpContext.getRetrieverContext())
              .build();

      entityService.ingestProposal(systemOpContext, batch, mode != EvaluationMode.SYNC);

      log.info(
          "Mode {}: Applying {} results to DataHub for test {}",
          mode,
          results.keySet().size(),
          testUrn);
      batchApplyActions(opContext, results, testDefinition, mode);
      log.info("Mode {}: Test {} evaluation done", mode, testUrn);
    }
    log.debug("Test {} has been evaluated. Results keys = {}", testUrn, results.keySet().size());
    return results;
  }

  private Map<Urn, TestResults> getUrnTestResultsMap(
      Urn testUrn,
      TestDefinition testDefinition,
      Map<Urn, BatchTestRunResult> batchTestRunResults,
      TestInfo testInfo) {
    final Map<Urn, TestResults> results;
    Map<Urn, TestResults> tempResults;
    final String query =
        testInfo.getDefinition().getOnQuery() != null ? testInfo.getDefinition().getOnQuery() : "*";
    // Prefer ElasticSearchTestExecutor if it can execute the test
    try {
      boolean canSelect =
          elasticSearchExecutorEnabled && elasticSearchTestExecutor.canSelect(testDefinition);
      if (canSelect && elasticSearchTestExecutor.canEvaluate(testDefinition)) {
        log.info("ElasticSearchTestExecutor can execute test {}", testUrn);
        tempResults =
            elasticSearchTestExecutor.evaluate(
                testDefinition,
                query,
                batchTestRunResults.get(testUrn),
                isBulkFormSubmission(testInfo));
        log.info("Test results size for test {} is {}", testUrn, tempResults.size());
      } else {
        final Set<Urn> candidateUrns = new HashSet<>();
        if (canSelect) {
          log.info("ElasticSearchTestExecutor can select test {}", testUrn);
          candidateUrns.addAll(elasticSearchTestExecutor.select(testDefinition, query));
        } else {
          if (isBulkFormSubmission(testInfo)) {
            throw new UnsupportedOperationException(
                "Tried to use default selector when evaluating bulk form submission test. Elastic selector is required.");
          }
          defaultSelect(testDefinition, testUrn, candidateUrns);
        }

        tempResults = defaultEvaluate(candidateUrns, testUrn, testDefinition, batchTestRunResults);
      }
    } catch (SelectionTooLargeException se) {
      // If we know the selection is too large, don't try to use default selector
      throw se;
    } catch (Exception e) {
      if (isBulkFormSubmission(testInfo)) {
        throw new UnsupportedOperationException(
            "Tried to use default selector when evaluating bulk form submission test. Elastic selector is required.");
      }
      // If ES Test executor fails try to do default
      log.warn("Unable to use ElasticSearchExecutor.", e);
      Set<Urn> candidateUrns = new HashSet<>();
      defaultSelect(testDefinition, testUrn, candidateUrns);
      tempResults = defaultEvaluate(candidateUrns, testUrn, testDefinition, batchTestRunResults);
    }

    results = tempResults;
    results.keySet().stream()
        .limit(1)
        .forEach(
            urn -> {
              StringBuilder sb = new StringBuilder();
              TestResults urnResults = results.get(urn);
              sb.append("Passing: ");
              urnResults
                  .getPassing()
                  .forEach(
                      passing -> {
                        sb.append(passing.toString()).append(",");
                      });
              sb.append("; Failing: ");
              urnResults
                  .getFailing()
                  .forEach(
                      failing -> {
                        sb.append(failing.toString()).append(",");
                      });
              log.info("Entity {} results: {}", urn, sb.toString());
            });
    return results;
  }

  public void defaultSelect(TestDefinition testDefinition, Urn testUrn, Set<Urn> candidateUrns) {
    log.info("ElasticSearchTestExecutor cannot select test {}", testUrn);
    testDefinition
        .getOn()
        .getEntityTypes()
        .forEach(
            typeName ->
                candidateUrns.addAll(
                    entityService
                        .listUrns(
                            systemOpContext, typeName, 0, executionLimits.getDefaultExecutor())
                        .getEntities()));
    if (candidateUrns.size() >= executionLimits.getDefaultExecutor()) {
      throw abortBeyondLimitExecution(testDefinition, candidateUrns.size());
    }
  }

  private Map<Urn, TestResults> defaultEvaluate(
      Set<Urn> candidateUrns,
      Urn testUrn,
      TestDefinition testDefinition,
      Map<Urn, BatchTestRunResult> batchTestRunResults) {
    if (candidateUrns.size() >= executionLimits.getDefaultExecutor()) {
      log.warn(
          "Test {} has too many entities to evaluate: {}. Will not execute.",
          testUrn,
          candidateUrns.size());
      throw abortBeyondLimitExecution(testDefinition, candidateUrns.size());
    }
    Set<Urn> urnsToTest =
        candidateUrns.stream()
            .limit(executionLimits.getDefaultExecutor())
            .collect(Collectors.toSet());

    return batchEvaluateTests(urnsToTest, Set.of(testDefinition), batchTestRunResults);
  }

  /**
   * Refreshes test definitions for a single test urn. Use when you want to make sure that the cache
   * is up to date for a single test
   *
   * @param testUrn test entity to refresh
   */
  public void refreshSingleTest(@Nonnull final Urn testUrn) {
    testRefreshRunnable.refreshOneUrn(testUrn);
  }

  private TestInfo getTestInfo(@Nonnull OperationContext opContext, @Nonnull final Urn testUrn) {
    TestInfo testInfo = null;

    try {
      EntityResponse response =
          entityService.getEntityV2(
              opContext, TEST_ENTITY_NAME, testUrn, ImmutableSet.of(TEST_INFO_ASPECT_NAME), false);
      if (response != null && response.getAspects().containsKey(TEST_INFO_ASPECT_NAME)) {
        testInfo = new TestInfo(response.getAspects().get(TEST_INFO_ASPECT_NAME).getValue().data());
      }
    } catch (Exception e) {
      log.error(String.format("Failed to fetch test info for test urn %s", testUrn));
    }
    return testInfo;
  }

  private static boolean isBulkFormSubmission(@Nonnull final TestInfo testInfo) {
    return testInfo.getSource() != null
        && TestSourceType.BULK_FORM_SUBMISSION.equals(testInfo.getSource().getType());
  }

  private static boolean isFormAssignmentTest(@Nonnull final TestInfo testInfo) {
    return testInfo.getSource() != null
        && TestSourceType.FORMS.equals(testInfo.getSource().getType());
  }

  private static boolean isFormPromptTest(@Nonnull final TestInfo testInfo) {
    return testInfo.getSource() != null
        && TestSourceType.FORM_PROMPT.equals(testInfo.getSource().getType());
  }

  static boolean shouldWriteAssetResults(@Nonnull final TestInfo testInfo) {
    return !isBulkFormSubmission(testInfo)
        && !isFormAssignmentTest(testInfo)
        && !isFormPromptTest(testInfo);
  }

  /**
   * A {@link Runnable} used to periodically fetch a new instance of the test Cache.
   *
   * <p>Currently, the refresh logic is not very smart. When the cache is invalidated, we simply
   * re-fetch the entire cache using Tests stored in the backend.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  class TestRefreshRunnable implements Runnable {

    private final OperationContext systemOpContext;
    private final TestFetcher _testFetcher;
    private final TestDefinitionParser _testDefinitionParser;
    private final Map<Urn, TestDefinition> _testCache;
    private final Map<Urn, TestInfo> _testInfoCache;
    private final Map<String, Set<TestDefinition>> _testPerEntityTypeCache;
    private final Lock cacheWriteLock;

    @Override
    public void run() {
      try {
        // Populate new cache and swap.
        Map<Urn, TestDefinition> newTestCache = new HashMap<>();
        Map<Urn, TestInfo> newTestInfoCache = new HashMap<>();
        Map<String, Set<TestDefinition>> newTestPerEntityCache = new HashMap<>();

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            final TestFetcher.TestFetchResult testFetchResult =
                _testFetcher.fetch(systemOpContext, start, count);

            addTestsToCache(
                newTestCache, newTestInfoCache, newTestPerEntityCache, testFetchResult.getTests());

            total = testFetchResult.getTotal();
            start = start + count;
          } catch (Exception e) {
            log.error(
                "Failed to retrieve test urns! Skipping updating test cache until next refresh. start: {}, count: {}",
                start,
                count,
                e);
            return;
          }
        }

        cacheWriteLock.lock();
        try {
          _testCache.clear();
          _testCache.putAll(newTestCache);
          _testInfoCache.clear();
          _testInfoCache.putAll(newTestInfoCache);
          _testPerEntityTypeCache.clear();
          _testPerEntityTypeCache.putAll(newTestPerEntityCache);
        } finally {
          // To unlock the acquired write thread
          cacheWriteLock.unlock();
        }

        log.debug(String.format("Successfully fetched %s tests.", total));
      } catch (Exception e) {
        log.error(
            "Caught exception while loading Test cache. Will retry on next scheduled attempt.", e);
      }
    }

    protected void refreshOneUrn(final Urn testUrn) {
      TestFetcher.TestFetchResult testDefinitions = _testFetcher.fetchOne(systemOpContext, testUrn);
      this.cacheWriteLock.lock();
      try {
        addTestsToCache(
            _testCache, _testInfoCache, _testPerEntityTypeCache, testDefinitions.getTests());
      } finally {
        // To unlock the acquired read thread
        cacheWriteLock.unlock();
      }
    }

    private void addTestsToCache(
        final Map<Urn, TestDefinition> testCache,
        final Map<Urn, TestInfo> testInfoCache,
        final Map<String, Set<TestDefinition>> testPerEntityTypeCache,
        final List<TestFetcher.Test> tests) {
      tests.forEach(test -> addTestToCache(testCache, testInfoCache, testPerEntityTypeCache, test));
    }

    private void addTestToCache(
        final Map<Urn, TestDefinition> testCache,
        final Map<Urn, TestInfo> testInfoCache,
        final Map<String, Set<TestDefinition>> cache,
        final TestFetcher.Test test) {
      // do not add tests without a schedule to the cache to prevent automatic runs
      if (test.getTestInfo().getSchedule() != null
          && test.getTestInfo().getSchedule().getInterval().equals(TestInterval.NONE)) {
        return;
      }

      TestDefinition testDefinition;
      try {
        testDefinition =
            _testDefinitionParser.deserialize(
                test.getUrn(), test.getTestInfo().getDefinition().getJson());
        ValidationResult validationResult = validateTestDefinition(testDefinition);
        if (!validationResult.isValid()) {
          throw new TestDefinitionParsingException(
              "Test definition "
                  + test.getUrn()
                  + " is invalid: "
                  + validationResult.getMessages());
        }
      } catch (TestDefinitionParsingException | IllegalArgumentException e) {
        log.error(
            "Issue while deserializing test definition {}",
            test.getTestInfo().getDefinition().getJson(),
            e);
        return;
      }
      testCache.put(test.getUrn(), testDefinition);

      testInfoCache.put(test.getUrn(), test.getTestInfo());

      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        Set<TestDefinition> existingTestsForEntityType =
            cache.getOrDefault(entityType, new HashSet<>());
        existingTestsForEntityType.add(testDefinition);
        cache.put(entityType, existingTestsForEntityType);
      }
    }
  }
}
