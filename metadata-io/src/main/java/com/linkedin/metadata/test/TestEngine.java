package com.linkedin.metadata.test;

import static com.linkedin.metadata.test.TestConstants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.TestAction;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.test.util.TestUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import datahub.client.patch.tests.TestResultsPatchBuilder;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Main engine responsible for evaluating all tests for all entities or for a given entity
 *
 * <p>Currently, the engine is implemented as a spring-instantiated Singleton which manages its own
 * thread-pool used for resolving tests.
 */
@Slf4j
public class TestEngine {
  private final EntityService _entityService;
  private final QueryEngine _queryEngine;
  private final PredicateEvaluator _predicateEvaluator;
  private final TestDefinitionParser _testDefinitionParser;
  private final ActionApplier _actionApplier;

  // Maps test urn to deserialized test definition
  // Not concurrent data structure because writes are always against the entire thing.
  private final Map<Urn, TestDefinition> _testCache = new HashMap<>();
  // Maps entity type to list of tests that target the entity type
  // Not concurrent data structure because writes are always against the entire thing.
  private final Map<String, Set<TestDefinition>> _testPerEntityTypeCache = new HashMap<>();

  // Shared lock for both cache hashmaps
  private final ReadWriteLock cacheReadWriteLock = new ReentrantReadWriteLock();
  private final Lock cacheReadLock = cacheReadWriteLock.readLock();

  private ScheduledExecutorService _refreshExecutorService;
  private final TestRefreshRunnable _testRefreshRunnable;
  private final Set<String> _supportedEntityTypes;
  private final boolean isPartialFetcher;

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
     * Default evaluation mode, where results are saved to storage async and actions are applied.
     */
    DEFAULT,

    /** Default evaluation mode, where results are saved to storage sync and actions are applied. */
    SYNC
  }

  public TestEngine(
      EntityService entityService,
      TestFetcher testFetcher,
      TestDefinitionParser testDefinitionParser,
      QueryEngine queryEngine,
      PredicateEvaluator predicateEvaluator,
      ActionApplier actionApplier,
      final int delayIntervalSeconds,
      final int refreshIntervalSeconds) {

    _entityService = entityService;
    _queryEngine = queryEngine;
    _predicateEvaluator = predicateEvaluator;
    _testDefinitionParser = testDefinitionParser;
    _actionApplier = actionApplier;
    isPartialFetcher = testFetcher.isPartial();
    _testRefreshRunnable =
        new TestRefreshRunnable(
            testFetcher,
            testDefinitionParser,
            _testCache,
            _testPerEntityTypeCache,
            cacheReadWriteLock.writeLock());

    if (refreshIntervalSeconds > 0) {
      _refreshExecutorService = Executors.newScheduledThreadPool(1);
      _refreshExecutorService.scheduleAtFixedRate(
          _testRefreshRunnable, delayIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
      _refreshExecutorService.execute(_testRefreshRunnable);
    } else {
      loadTests();
    }
    _supportedEntityTypes = TestUtils.getSupportedEntityTypes(entityService.getEntityRegistry());
  }

  /**
   * Invalidates the test cache and fires off a refresh thread. Should be invoked when a test is
   * created, modified, or deleted.
   */
  public void invalidateCache() {
    if (_refreshExecutorService != null) {
      _refreshExecutorService.execute(_testRefreshRunnable);
    }
  }

  /** Synchronously refresh the Metadata Tests cache. */
  public void loadTests() {
    _testRefreshRunnable.run();
  }

  @Nonnull
  public Set<String> getEntityTypesToEvaluate() {
    cacheReadLock.lock();
    try {
      return _testPerEntityTypeCache.keySet();
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
  }

  @Nonnull
  public TestDefinitionParser getParser() {
    return _testDefinitionParser;
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
      testDefinition = _testDefinitionParser.deserialize(DUMMY_TEST_URN, definitionJson);
    } catch (TestDefinitionParsingException e) {
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
  public TestResults evaluateTests(@Nonnull final Urn urn, @Nonnull EvaluationMode mode) {
    return evaluateTests(Set.of(urn), Set.of(), mode).getOrDefault(urn, EMPTY_RESULTS);
  }

  /**
   * Evaluate input tests for the given urn, then optionally write results to GMS. If no eligible
   * tests are found for the urn, an empty set of test results will be returned.
   *
   * @param urn Entity urn to evaluate
   * @param testUrns Tests to evaluate
   * @param mode Whether or not to push the test results into DataHub
   * @return Test results.
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  @Nonnull
  public TestResults evaluateTestUrns(
      @Nonnull final Urn urn, @Nonnull Set<Urn> testUrns, @Nonnull EvaluationMode mode) {
    return evaluateTestUrns(Set.of(urn), testUrns, mode).getOrDefault(urn, EMPTY_RESULTS);
  }

  /**
   * Batch evaluate all eligible tests for input set of urns.
   *
   * @param urns Entity urns to evaluate
   * @param mode The evaluation mode
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> evaluateTests(
      @Nonnull final Set<Urn> urns, @Nonnull final EvaluationMode mode) {
    return evaluateTests(urns, Set.of(), mode);
  }

  public Map<Urn, TestResults> evaluateTestUrns(
      @Nonnull final Set<Urn> entityUrns,
      @Nonnull final Set<Urn> testUrns,
      @Nonnull EvaluationMode mode) {
    return evaluateTests(
        entityUrns, testUrns.isEmpty() ? Set.of() : fetchDefinitions(testUrns), mode);
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
      if (!_supportedEntityTypes.contains(entityType)) {
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

    // Step 2: Evaluate all tests for entity
    Map<Urn, TestResults> results = batchEvaluateTests(entityUrns, testDefinitions);

    // Step 3 (Optional): Write results to DataHub
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      batchIngestResults(results, mode, partial);
      batchApplyActions(results);
    }

    return results;
  }

  /**
   * Evaluates tests against a set of entity urns in batch.
   *
   * @param urns the urns to be tested
   * @param tests the test definitions to be evaluated
   * @return a map of urn to the corresponding test results.
   */
  @WithSpan
  private Map<Urn, TestResults> batchEvaluateTests(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<TestDefinition> tests) {
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

      // Batch evaluate all queries in the main rules for all eligible entities
      Map<Urn, Map<TestQuery, TestQueryResponse>> rulesQueryResponses =
          batchQuery(urnsToTest, ImmutableList.of(testDefinition.getRules()));

      // For each entity, evaluate whether it passes the test using the query evaluation results
      for (Urn urn : urnsToTest) {

        // Run the test!
        final boolean isUrnPassingTest =
            _predicateEvaluator.evaluatePredicate(
                testDefinition.getRules(),
                rulesQueryResponses.getOrDefault(urn, Collections.emptyMap()));

        if (isUrnPassingTest) {
          finalTestResults
              .get(urn)
              .getPassing()
              .add(
                  new TestResult()
                      .setTest(testDefinition.getUrn())
                      .setType(TestResultType.SUCCESS));
        } else {
          finalTestResults
              .get(urn)
              .getFailing()
              .add(
                  new TestResult()
                      .setTest(testDefinition.getUrn())
                      .setType(TestResultType.FAILURE));
        }
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
    // Always make sure we batch queries together as much as possible to reduce calls
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
      queries.addAll(
          _predicateEvaluator.extractQueriesForPredicate(testDefinition.getOn().getConditions()));
    }

    // Validate 'rules' block queries.
    queries.addAll(_predicateEvaluator.extractQueriesForPredicate(testDefinition.getRules()));

    // Verify that each defined query is valid. If multiple are invalid, merge the error messages
    List<ValidationResult> invalidResults =
        queries.stream()
            .map(query -> _queryEngine.validateQuery(query, entityTypes))
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
            .flatMap(rule -> _predicateEvaluator.extractQueriesForPredicate(rule).stream())
            .collect(Collectors.toSet());
    return _queryEngine.batchEvaluateQueries(new HashSet<>(urns), requiredQueries);
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
        if (_testCache.containsKey(testUrn)) {
          tests.add(_testCache.get(testUrn));
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
    return _predicateEvaluator.evaluatePredicate(selectConditions, targetingRulesQueryResponses);
  }

  /**
   * Overwrites test results into GMS for a given urn.
   *
   * @param testResults Test result map
   * @param mode Evaluation mode
   */
  private void batchIngestResults(
      @Nonnull Map<Urn, TestResults> testResults, EvaluationMode mode, boolean partial) {
    final List<MetadataChangeProposal> mcps;

    if (partial) {
      mcps =
          testResults.entrySet().stream()
              .map(
                  entry ->
                      new TestResultsPatchBuilder()
                          .urn(entry.getKey())
                          .updateTestResults(entry.getValue()))
              .filter(TestResultsPatchBuilder::hasPatchOperations)
              .map(TestResultsPatchBuilder::build)
              .collect(Collectors.toList());
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
                  })
              .collect(Collectors.toList());
    }

    if (!mcps.isEmpty()) {
      AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      AspectsBatchImpl batch =
          AspectsBatchImpl.builder()
              .mcps(
                  mcps,
                  auditStamp,
                  _entityService.getEntityRegistry(),
                  _entityService.getSystemEntityClient())
              .build();

      _entityService.ingestProposal(batch, mode != EvaluationMode.SYNC);
    }
  }

  /**
   * Batch applies actions for an set of entities.
   *
   * <p>To do this, we first group by the Action -> entity URNs requiring the action. Then, we batch
   * apply the action to all entity URNs in the set using an {@link ActionApplier}.
   */
  private void batchApplyActions(@Nonnull final Map<Urn, TestResults> entityUrnToResults) {
    // First, aggregate by Action -> URNs, so we can batch the action execution itself.
    Map<TestAction, Set<Urn>> actionToEntityUrns = new HashMap<>();
    for (Map.Entry<Urn, TestResults> entry : entityUrnToResults.entrySet()) {
      addActionsToEntityMap(
          entry.getKey(), entry.getValue().getPassing(), actionToEntityUrns); // Passing Actions
      addActionsToEntityMap(
          entry.getKey(), entry.getValue().getFailing(), actionToEntityUrns); // Failing Actions
    }

    // Apply the action for each batch.
    for (Map.Entry<TestAction, Set<Urn>> entry : actionToEntityUrns.entrySet()) {
      _actionApplier.apply(
          entry.getKey().getType(),
          new ArrayList<>(entry.getValue()),
          new ActionParameters(entry.getKey().getParams()));
    }
  }

  /** Batch applies actions for an entity. */
  private void addActionsToEntityMap(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<TestResult> testResults,
      @Nonnull final Map<TestAction, Set<Urn>> actionToEntityUrns) {

    cacheReadLock.lock();
    try {
      for (TestResult result : testResults) {
        TestDefinition definition = _testCache.get(result.getTest());
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
      return _testPerEntityTypeCache.getOrDefault(key, defaultValue);
    } finally {
      // To unlock the acquired read thread
      cacheReadLock.unlock();
    }
  }

  /**
   * A {@link Runnable} used to periodically fetch a new instance of the test Cache.
   *
   * <p>Currently, the refresh logic is not very smart. When the cache is invalidated, we simply
   * re-fetch the entire cache using Tests stored in the backend.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  static class TestRefreshRunnable implements Runnable {

    private final TestFetcher _testFetcher;
    private final TestDefinitionParser _testDefinitionParser;
    private final Map<Urn, TestDefinition> _testCache;
    private final Map<String, Set<TestDefinition>> _testPerEntityTypeCache;
    private final Lock cacheWriteLock;

    @Override
    public void run() {
      try {
        // Populate new cache and swap.
        Map<Urn, TestDefinition> newTestCache = new HashMap<>();
        Map<String, Set<TestDefinition>> newTestPerEntityCache = new HashMap<>();

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            final TestFetcher.TestFetchResult testFetchResult = _testFetcher.fetch(start, count);

            addTestsToCache(newTestCache, newTestPerEntityCache, testFetchResult.getTests());

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

    private void addTestsToCache(
        final Map<Urn, TestDefinition> testCache,
        final Map<String, Set<TestDefinition>> testPerEntityTypeCache,
        final List<TestFetcher.Test> tests) {
      tests.forEach(test -> addTestToCache(testCache, testPerEntityTypeCache, test));
    }

    private void addTestToCache(
        final Map<Urn, TestDefinition> testCache,
        final Map<String, Set<TestDefinition>> cache,
        final TestFetcher.Test test) {
      TestDefinition testDefinition;
      try {
        testDefinition =
            _testDefinitionParser.deserialize(
                test.getUrn(), test.getTestInfo().getDefinition().getJson());
      } catch (TestDefinitionParsingException e) {
        log.error(
            "Issue while deserializing test definition {}",
            test.getTestInfo().getDefinition().getJson(),
            e);
        return;
      }
      testCache.put(test.getUrn(), testDefinition);
      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        Set<TestDefinition> existingTestsForEntityType =
            cache.getOrDefault(entityType, new HashSet<>());
        existingTestsForEntityType.add(testDefinition);
        cache.put(entityType, existingTestsForEntityType);
      }
    }
  }
}
