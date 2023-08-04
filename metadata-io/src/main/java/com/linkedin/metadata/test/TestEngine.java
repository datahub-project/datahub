package com.linkedin.metadata.test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
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
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.test.TestConstants.*;


/**
 * Main engine responsible for evaluating all tests for all entities or for a given entity
 *
 * Currently, the engine is implemented as a spring-instantiated Singleton
 * which manages its own thread-pool used for resolving tests.
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
  private final Map<String, List<TestDefinition>> _testPerEntityTypeCache = new HashMap<>();

  private final ScheduledExecutorService _refreshExecutorService = Executors.newScheduledThreadPool(1);
  private final TestRefreshRunnable _testRefreshRunnable;
  private final Set<String> _supportedEntityTypes;

  /**
   * The test engine evaluation mode.
   */
  public enum EvaluationMode {
    /**
     * A transient evaluation, where Test Results are not stored anywhere,
     * actions are not applied, and results are simply returned to the client.
     *
     * This is useful for trying out a Metadata Test definition before it's
     * configured completely.
     */
    EVALUATE_ONLY,

    /**
     * Default evaluation mode, where results are saved to storage and
     * actions are applied.
     */
    DEFAULT
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
    _testRefreshRunnable =
        new TestRefreshRunnable(testFetcher, testDefinitionParser, _testCache, _testPerEntityTypeCache);
    _refreshExecutorService.scheduleAtFixedRate(_testRefreshRunnable, delayIntervalSeconds, refreshIntervalSeconds,
        TimeUnit.SECONDS);
    _refreshExecutorService.execute(_testRefreshRunnable);
    _supportedEntityTypes = TestUtils.getSupportedEntityTypes(entityService.getEntityRegistry());
  }

  /**
   * Invalidates the test cache and fires off a refresh thread. Should be invoked
   * when a test is created, modified, or deleted.
   */
  public void invalidateCache() {
    _refreshExecutorService.execute(_testRefreshRunnable);
  }

  /**
   * Synchronously refresh the Metadata Tests cache.
   */
  public void loadTests() {
    _testRefreshRunnable.run();
  }

  @Nonnull
  public List<TestDefinition> getTestDefinitions() {
    return _testPerEntityTypeCache.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  @Nonnull
  public Set<String> getEntityTypesToEvaluate() {
    return _testPerEntityTypeCache.keySet();
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
   * Evaluate all eligible tests for the given urn, optionally write the results to GMS.
   * If no eligible tests are found for the urn, an empty set of test results will be returned.
   *
   * @param urn Entity urn to evaluate
   * @param mode The evaluation mode.
   * @return Test results
   *
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  @Nonnull
  public TestResults evaluateTestsForEntity(@Nonnull final Urn urn, EvaluationMode mode) {
    if (!_supportedEntityTypes.contains(urn.getEntityType())) {
      log.warn(String.format("Attempted to evaluate tests for an unsupported entity type %s. Returning null results.", urn.getEntityType()));
      throw new UnsupportedOperationException(
          String.format("Attempted to evaluate tests for an unsupported entity type %s. Returning null results.",
              urn.getEntityType()));
    }

    // Step 1: Retrieve eligible tests.
    List<TestDefinition> testsEligibleForEntityType = _testPerEntityTypeCache.getOrDefault(
        urn.getEntityType(),
        Collections.emptyList());

    // Step 2: Evaluate all tests for entity
    TestResults results = evaluateTests(urn, testsEligibleForEntityType);

    // Step 3 (Optional): Write results to DataHub
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      ingestResults(urn, results);
      batchApplyActions(ImmutableMap.of(urn, results));
    }
    return results;
  }

  /**
   * Evaluate input tests for the given urn, then optionally write results to GMS.
   * If no eligible tests are found for the urn, an empty set of test results will be returned.
   *
   * @param urn Entity urn to evaluate
   * @param testUrns Tests to evaluate
   * @param mode Whether or not to push the test results into DataHub
   * @return Test results.
   *
   * @throws UnsupportedOperationException if the provided entity type is not supported.
   */
  @Nonnull
  public TestResults evaluateTests(Urn urn, List<Urn> testUrns, EvaluationMode mode) {
    if (!_supportedEntityTypes.contains(urn.getEntityType())) {
      log.warn(String.format("Attempted to evaluate tests for an unsupported entity type %s. Returning null results.", urn.getEntityType()));
      throw new UnsupportedOperationException(
          String.format("Attempted to evaluate tests for an unsupported entity type %s. Returning null results.",
          urn.getEntityType()));
    }
    TestResults results = evaluateTests(urn, fetchDefinitions(testUrns));
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      ingestPartialResults(urn, testUrns, results);
      batchApplyActions(ImmutableMap.of(urn, results));
    }
    return results;
  }

  /**
   * Evaluate a specific set of tests for a given entity urn, then return the results.
   *
   * TODO: Combine this with batchEvaluateTests
   *
   * @param urn the urn to test
   * @param tests the list of tests to execute against the urn
   * @return the results of running the tests
   */
  @WithSpan
  @Nonnull
  public TestResults evaluateTests(@Nonnull final Urn urn, @Nonnull final List<TestDefinition> tests) {
    if (tests.isEmpty()) {
      return EMPTY_RESULTS;
    }

    log.debug(String.format("Found complete set of test definitions tests: %s", tests));

    List<TestDefinition> eligibleTests = evaluateSelect(urn, tests);

    log.debug(String.format("Found eligible tests for urn %s, tests: %s", urn, eligibleTests));

    TestResults result = new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray());

    // Batch evaluate all queries in the main rules
    Map<TestQuery, TestQueryResponse> mainRulesQueryResponses = batchQuery(ImmutableList.of(urn),
        eligibleTests.stream().map(TestDefinition::getRules).collect(Collectors.toList())).getOrDefault(urn,
        Collections.emptyMap());

    // Evaluate whether each test passes using the query evaluation result above (no service calls. pure computation)
    for (TestDefinition eligibleTest : eligibleTests) {
      if (_predicateEvaluator.evaluatePredicate(eligibleTest.getRules(), mainRulesQueryResponses)) {
        result.getPassing().add(new TestResult().setTest(eligibleTest.getUrn()).setType(TestResultType.SUCCESS));
      } else {
        result.getFailing().add(new TestResult().setTest(eligibleTest.getUrn()).setType(TestResultType.FAILURE));
      }
    }

    log.debug(String.format("Finished executing eligible tests for urn %s, results: %s", urn, result));

    return result;
  }

  /**
   * Batch evaluate all eligible tests for input set of urns.
   *
   * @param urns Entity urns to evaluate
   * @param mode The evaluation mode
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> batchEvaluateTestsForEntities(
      @Nonnull final List<Urn> urns,
      @Nonnull final EvaluationMode mode) {
    Map<String, List<Urn>> urnsPerEntityType = urns.stream().collect(Collectors.groupingBy(Urn::getEntityType));
    Map<Urn, TestResults> finalTestResults = new HashMap<>();

    for (String entityType : urnsPerEntityType.keySet()) {
      List<TestDefinition> testsEligibleForEntityType = _testPerEntityTypeCache.getOrDefault(entityType, Collections.emptyList());
      if (testsEligibleForEntityType.isEmpty()) {
        continue;
      }

      // TODO: Evaluate whether the next line has a bug in the urns to evaluate.
      Map<Urn, TestResults> resultsForEntityType = batchEvaluateTests(urns, testsEligibleForEntityType);
      finalTestResults.putAll(resultsForEntityType);
    }

    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      finalTestResults.forEach(this::ingestResults);
      batchApplyActions(finalTestResults);
    }

    return finalTestResults;
  }

  /**
   * Batch evaluate a specific set of tests tests for input set of urns
   *
   * @param urns Entity urns to evaluate
   * @param testUrns Tests to evaluate
   * @param mode The evaluation mode
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> batchEvaluateTests(
      @Nonnull final List<Urn> urns,
      @Nonnull final List<Urn> testUrns,
      @Nonnull final EvaluationMode mode) {
    final Map<Urn, TestResults> resultsPerUrn = batchEvaluateTests(urns, fetchDefinitions(testUrns));
    if (!EvaluationMode.EVALUATE_ONLY.equals(mode)) {
      resultsPerUrn.forEach((urn, results) -> ingestPartialResults(urn, testUrns, results));
      batchApplyActions(resultsPerUrn);
    }
    return resultsPerUrn;
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
      queries.addAll(_predicateEvaluator.extractQueriesForPredicate(testDefinition.getOn().getConditions()));
    }

    // Validate 'rules' block queries.
    queries.addAll(_predicateEvaluator.extractQueriesForPredicate(testDefinition.getRules()));

    // Verify that each defined query is valid. If multiple are invalid, merge the error messages
    List<ValidationResult> invalidResults = queries.stream()
        .map(query -> _queryEngine.validateQuery(query, entityTypes))
        .filter(result -> !result.isValid())
        .collect(Collectors.toList());

    if (invalidResults.isEmpty()) {
      return ValidationResult.validResult();
    }
    return new ValidationResult(
        false,
        invalidResults.stream().flatMap(result -> result.getMessages().stream()).collect(Collectors.toList()));
  }

  /**
   * Evaluates tests against a set of entity urns in batch.
   *
   * @param urns the urns to be tested
   * @param tests the test definitions to be evaluated
   * @return a map of urn to the corresponding test results.
   */
  @WithSpan
  private Map<Urn, TestResults> batchEvaluateTests(@Nonnull final List<Urn> urns, @Nonnull final List<TestDefinition> tests) {
    if (tests.isEmpty()) {
      return urns.stream().collect(Collectors.toMap(Function.identity(), urn -> EMPTY_RESULTS));
    }

    // Get a map of test definition -> urns which match select conditions
    Map<TestDefinition, List<Urn>> eligibleTestsPerEntity = getEligibleEntitiesPerTest(urns, tests);

    Map<Urn, TestResults> finalTestResults = urns.stream()
        .collect(Collectors.toMap(Function.identity(),
            urn -> new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray())));

    // For each test with eligible entities
    for (TestDefinition testDefinition : eligibleTestsPerEntity.keySet()) {
      List<Urn> urnsToTest = eligibleTestsPerEntity.get(testDefinition);

      // Batch evaluate all queries in the main rules for all eligible entities
      Map<Urn, Map<TestQuery, TestQueryResponse>> rulesQueryResponses = batchQuery(urnsToTest, ImmutableList.of(testDefinition.getRules()));

      // For each entity, evaluate whether it passes the test using the query evaluation results
      for (Urn urn : urnsToTest) {

        // Run the test!
        final boolean isUrnPassingTest = _predicateEvaluator.evaluatePredicate(
            testDefinition.getRules(),
            rulesQueryResponses.getOrDefault(urn, Collections.emptyMap()));

        if (isUrnPassingTest) {
          finalTestResults.get(urn)
              .getPassing()
              .add(new TestResult().setTest(testDefinition.getUrn()).setType(TestResultType.SUCCESS));
        } else {
          finalTestResults.get(urn)
              .getFailing()
              .add(new TestResult().setTest(testDefinition.getUrn()).setType(TestResultType.FAILURE));
        }
      }
    }
    return finalTestResults;
  }

  /**
   * For each test, return the list of eligible entities based on matching against the select conditions.
   *
   * @param urns the entity urns
   * @param tests the list of test definitions
   * @return a map of the test definition to the URNs which should be evaluated.
   */
  private Map<TestDefinition, List<Urn>> getEligibleEntitiesPerTest(@Nonnull List<Urn> urns, @Nonnull List<TestDefinition> tests) {
    // First batch evaluate all queries in the targeting rules
    // Always make sure we batch queries together as much as possible to reduce calls
    Map<Urn, Map<TestQuery, TestQueryResponse>> selectConditionQueryResponse = batchQuery(urns, getPredicatesFromSelectConditions(tests));
    Map<TestDefinition, List<Urn>> eligibleTestsPerEntity = new HashMap<>();

    // Using the query evaluation result, find the set of eligible tests per entity.
    // Reverse map to get the list of entities eligible for each test
    for (Urn urn : urns) {
      Map<TestQuery, TestQueryResponse> queryResponseForUrn = selectConditionQueryResponse.getOrDefault(urn, Collections.emptyMap());
      tests.stream().filter(test -> evaluateSelectConditions(urn, queryResponseForUrn, test)).forEach(test -> {
        if (!eligibleTestsPerEntity.containsKey(test)) {
          eligibleTestsPerEntity.put(test, new ArrayList<>());
        }
        eligibleTestsPerEntity.get(test).add(urn);
      });
    }
    return eligibleTestsPerEntity;
  }

  @WithSpan
  private Map<Urn, Map<TestQuery, TestQueryResponse>> batchQuery(@Nonnull final List<Urn> urns, @Nonnull final List<Predicate> rules) {
    if (rules.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<TestQuery> requiredQueries = rules.stream()
        .flatMap(rule -> _predicateEvaluator.extractQueriesForPredicate(rule).stream())
        .collect(Collectors.toSet());
    return _queryEngine.batchEvaluateQueries(new HashSet<>(urns), requiredQueries);
  }

  private List<Predicate> getPredicatesFromSelectConditions(@Nonnull final List<TestDefinition> tests) {
    return tests.stream()
        .map(test -> test.getOn().getConditions())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private List<TestDefinition> fetchDefinitions(@Nonnull final List<Urn> testUrns) {
    List<TestDefinition> tests = new ArrayList<>(testUrns.size());
    for (Urn testUrn : testUrns) {
      if (_testCache.containsKey(testUrn)) {
        tests.add(_testCache.get(testUrn));
      } else {
        log.warn("Test {} does not exist: Skipping", testUrn);
      }
    }
    return tests;
  }

  // Get eligible tests for the given entity based on the select conditions
  private List<TestDefinition> evaluateSelect(@Nonnull final Urn urn, @Nonnull final List<TestDefinition> tests) {

    // First test against the 'types' in the on clause.
    final List<TestDefinition> testsToExecute = tests.stream()
        .filter(test -> evaluateSelectTypes(urn, test))
        .collect(Collectors.toList());

    // Then, resolve queries required in select conditions
    // Always make sure we batch queries together as much as possible to reduce calls
    Map<TestQuery, TestQueryResponse> selectConditionsQueryResponse = batchQuery(
        ImmutableList.of(urn),
        getPredicatesFromSelectConditions(testsToExecute)).getOrDefault(urn, Collections.emptyMap());

    // Based on the query evaluation result, find the list of tests that are eligible for the given urn
    return testsToExecute.stream()
        .filter(test -> evaluateSelectConditions(urn, selectConditionsQueryResponse, test))
        .collect(Collectors.toList());
  }

  /**
   * Evaluate whether the entity passes the select types in the test
   *
   * @param urn Entity urn in question
   * @param test Test that we are trying to evaluate
   * @return Whether or not the entity passes the types select clause
   */
  private boolean evaluateSelectTypes(Urn urn, TestDefinition test) {
    return test.getOn().getEntityTypes().contains(urn.getEntityType());
  }

  /**
   * Evaluate whether the entity passes the select rules in the test.
   * Note, we batch evaluate queries before calling this function, and the results are inputted as arguments
   * This function purely uses the query evaluation results and checks whether it passes the targeting rules or not
   *
   * @param urn Entity urn in question
   * @param targetingRulesQueryResponses Batch query evaluation results.
   *                                     Contains the result of all queries in the targeting rules
   * @param test Test that we are trying to evaluate
   * @return Whether or not the entity passes the targeting rules of the test
   */
  private boolean evaluateSelectConditions(Urn urn, Map<TestQuery, TestQueryResponse> targetingRulesQueryResponses,
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
   * Merges and writes test results into GMS for a given urn.
   *
   * @param urn the entity urn
   * @param testUrns the test urns
   * @param results the test results
   */
  private void ingestPartialResults(Urn urn, List<Urn> testUrns, TestResults results) {
    EntityResponse entityResponse;
    try {
      entityResponse =
          _entityService.getEntityV2(urn.getEntityType(), urn, ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME));
    } catch (URISyntaxException e) {
      log.error("Failed to fetch TestResults aspect for urn {}", urn, e);
      return;
    }
    // If TestResults aspect does not exist, do a simple ingest
    if (entityResponse == null || !entityResponse.getAspects().containsKey(Constants.TEST_RESULTS_ASPECT_NAME)) {
      ingestResults(urn, results);
      return;
    }
    TestResults prevResults = new TestResults(entityResponse.getAspects().get(Constants.TEST_RESULTS_ASPECT_NAME).getValue().data());
    TestResults mergedResults = new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray());
    // Add in current results
    mergedResults.getPassing().addAll(results.getPassing());
    mergedResults.getFailing().addAll(results.getFailing());
    // Add in previous results filtering out the tests that were just evaluated
    Set<Urn> testSet = new HashSet<>(testUrns);
    prevResults.getPassing()
        .stream()
        .filter(test -> !testSet.contains(test.getTest()))
        .forEach(mergedResults.getPassing()::add);
    prevResults.getFailing()
        .stream()
        .filter(test -> !testSet.contains(test.getTest()))
        .forEach(mergedResults.getFailing()::add);
    ingestResults(urn, mergedResults);
  }

  /**
   * Overwrites test results into GMS for a given urn.
   *
   * @param urn the entity urn
   * @param results the test results
   */
  private void ingestResults(Urn urn, TestResults results) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.TEST_RESULTS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(results));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()), false);
  }

  /**
   * Batch applies actions for an set of entities.
   *
   * To do this, we first group by the Action -> entity URNs requiring the action.
   * Then, we batch apply the action to all entity URNs in the set using an {@link ActionApplier}.
   */
  private void batchApplyActions(@Nonnull final Map<Urn, TestResults> entityUrnToResults) {
    // First, aggregate by Action -> URNs, so we can batch the action execution itself.
    Map<TestAction, Set<Urn>> actionToEntityUrns = new HashMap<>();
    for (Map.Entry<Urn, TestResults> entry : entityUrnToResults.entrySet()) {
      addActionsToEntityMap(entry.getKey(), entry.getValue().getPassing(), actionToEntityUrns); // Passing Actions
      addActionsToEntityMap(entry.getKey(), entry.getValue().getFailing(), actionToEntityUrns); // Failing Actions
    }

    // Apply the action for each batch.
    for (Map.Entry<TestAction, Set<Urn>> entry : actionToEntityUrns.entrySet()) {
      _actionApplier.apply(entry.getKey().getType(), new ArrayList<>(entry.getValue()), new ActionParameters(entry.getKey().getParams()));
    }
  }

  /**
   * Batch applies actions for an entity.
   */
  private void addActionsToEntityMap(
      @Nonnull final Urn entityUrn,
      @Nonnull final List<TestResult> testResults,
      @Nonnull final Map<TestAction, Set<Urn>> actionToEntityUrns) {
    for (TestResult result : testResults) {
      TestDefinition definition = _testCache.get(result.getTest());
      List<TestAction> eligibleActions = TestResultType.FAILURE.equals(result.getType())
          ? definition.getActions().getFailing()
          : definition.getActions().getPassing();
      for (TestAction action : eligibleActions) {
        actionToEntityUrns.putIfAbsent(action, new HashSet<>());
        actionToEntityUrns.get(action).add(entityUrn);
      }
    }
  }

  /**
   * A {@link Runnable} used to periodically fetch a new instance of the test Cache.
   *
   * Currently, the refresh logic is not very smart. When the cache is invalidated, we simply re-fetch the
   * entire cache using Tests stored in the backend.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  static class TestRefreshRunnable implements Runnable {

    private final TestFetcher _testFetcher;
    private final TestDefinitionParser _testDefinitionParser;
    private final Map<Urn, TestDefinition> _testCache;
    private final Map<String, List<TestDefinition>> _testPerEntityTypeCache;

    @Override
    public void run() {
      try {
        // Populate new cache and swap.
        Map<Urn, TestDefinition> newTestCache = new HashMap<>();
        Map<String, List<TestDefinition>> newTestPerEntityCache = new HashMap<>();

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
                start, count, e);
            return;
          }
          synchronized (_testCache) {
            _testCache.clear();
            _testCache.putAll(newTestCache);
          }
          synchronized (_testPerEntityTypeCache) {
            _testPerEntityTypeCache.clear();
            _testPerEntityTypeCache.putAll(newTestPerEntityCache);
          }
        }
        log.debug(String.format("Successfully fetched %s tests.", total));
      } catch (Exception e) {
        log.error("Caught exception while loading Test cache. Will retry on next scheduled attempt.", e);
      }
    }

    private void addTestsToCache(final Map<Urn, TestDefinition> testCache,
        final Map<String, List<TestDefinition>> testPerEntityTypeCache, final List<TestFetcher.Test> tests) {
      tests.forEach(test -> addTestToCache(testCache, testPerEntityTypeCache, test));
    }

    private void addTestToCache(final Map<Urn, TestDefinition> testCache, final Map<String, List<TestDefinition>> cache,
        final TestFetcher.Test test) {
      TestDefinition testDefinition;
      try {
        testDefinition =
            _testDefinitionParser.deserialize(test.getUrn(), test.getTestInfo().getDefinition().getJson());
      } catch (TestDefinitionParsingException e) {
        log.error("Issue while deserializing test definition {}", test.getTestInfo().getDefinition().getJson(), e);
        return;
      }
      testCache.put(test.getUrn(), testDefinition);
      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        List<TestDefinition> existingTestsForEntityType = cache.getOrDefault(entityType, new ArrayList<>());
        existingTestsForEntityType.add(testDefinition);
        cache.put(entityType, existingTestsForEntityType);
      }
    }
  }
}
