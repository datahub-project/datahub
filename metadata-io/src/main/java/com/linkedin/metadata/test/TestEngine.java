package com.linkedin.metadata.test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.TestDefinitionProvider;
import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.eval.TestPredicateEvaluator;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


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
  private final TestPredicateEvaluator _testPredicateEvaluator;
  private final TestDefinitionProvider _testDefinitionProvider;

  // Maps test urn to deserialized test definition
  // Not concurrent data structure because writes are always against the entire thing.
  private final Map<Urn, TestDefinition> _testCache = new HashMap<>();
  // Maps entity type to list of tests that target the entity type
  // Not concurrent data structure because writes are always against the entire thing.
  private final Map<String, List<TestDefinition>> _testPerEntityTypeCache = new HashMap<>();

  private final ScheduledExecutorService _refreshExecutorService = Executors.newScheduledThreadPool(1);
  private final TestRefreshRunnable _testRefreshRunnable;

  private static final TestResults EMPTY_RESULTS =
      new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray());
  private static final Urn DUMMY_TEST_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new TestKey().setId("dummy"), Constants.TEST_ENTITY_NAME);

  public TestEngine(EntityService entityService, TestFetcher testFetcher, TestDefinitionProvider testDefinitionProvider,
      QueryEngine queryEngine, TestPredicateEvaluator testPredicateEvaluator, final int delayIntervalSeconds,
      final int refreshIntervalSeconds) {
    _entityService = entityService;
    _queryEngine = queryEngine;
    _testPredicateEvaluator = testPredicateEvaluator;
    _testDefinitionProvider = testDefinitionProvider;
    _testRefreshRunnable =
        new TestRefreshRunnable(testFetcher, testDefinitionProvider, _testCache, _testPerEntityTypeCache);
    _refreshExecutorService.scheduleAtFixedRate(_testRefreshRunnable, delayIntervalSeconds, refreshIntervalSeconds,
        TimeUnit.SECONDS);
    _refreshExecutorService.execute(_testRefreshRunnable);
  }

  /**
   * Invalidates the test cache and fires off a refresh thread. Should be invoked
   * when a test is created, modified, or deleted.
   */
  public void invalidateCache() {
    _refreshExecutorService.execute(_testRefreshRunnable);
  }

  public Set<String> getEntityTypesToEvaluate() {
    return _testPerEntityTypeCache.keySet();
  }

  @Nonnull
  public ValidationResult validateJson(String definitionJson) {
    // Try to deserialize json definition
    TestDefinition testDefinition;
    try {
      testDefinition = _testDefinitionProvider.deserialize(DUMMY_TEST_URN, definitionJson);
    } catch (TestDefinitionParsingException e) {
      return new ValidationResult(false, Collections.singletonList(e.getMessage()));
    }
    return validateTestDefinition(testDefinition);
  }

  // Validate all queries in the test definition
  private ValidationResult validateTestDefinition(TestDefinition testDefinition) {
    List<String> entityTypes = testDefinition.getTarget().getEntityTypes();
    List<TestQuery> queries = new ArrayList<>();
    if (testDefinition.getTarget().getTargetingRules().isPresent()) {
      queries.addAll(_testPredicateEvaluator.getRequiredQueries(testDefinition.getTarget().getTargetingRules().get()));
    }
    queries.addAll(_testPredicateEvaluator.getRequiredQueries(testDefinition.getRule()));
    // Make sure every query in the test definition is valid. If any are invalid, merge the error messages
    List<ValidationResult> invalidResults = queries.stream()
        .map(query -> _queryEngine.validateQuery(query, entityTypes))
        .filter(result -> !result.isValid())
        .collect(Collectors.toList());
    if (invalidResults.isEmpty()) {
      return ValidationResult.validResult();
    }
    return new ValidationResult(false,
        invalidResults.stream().flatMap(result -> result.getMessages().stream()).collect(Collectors.toList()));
  }

  /**
   * Evaluate all eligible tests for the given urn
   *
   * @param urn Entity urn to evaluate
   * @param shouldPush Whether or not to push the test results into DataHub
   * @return Test results
   */
  public TestResults evaluateTestsForEntity(Urn urn, boolean shouldPush) {
    List<TestDefinition> testsEligibleForEntityType =
        _testPerEntityTypeCache.getOrDefault(urn.getEntityType(), Collections.emptyList());
    TestResults results = evaluateTests(urn, testsEligibleForEntityType);

    if (shouldPush) {
      ingestResults(urn, results);
    }
    return results;
  }

  /**
   * Evaluate input tests for the given urn
   *
   * @param urn Entity urn to evaluate
   * @param testUrns Tests to evaluate
   * @param shouldPush Whether or not to push the test results into DataHub
   * @return Test results
   */
  public TestResults evaluateTests(Urn urn, List<Urn> testUrns, boolean shouldPush) {
    TestResults results = evaluateTests(urn, fetchDefinitions(testUrns));

    if (shouldPush) {
      ingestPartialResults(urn, testUrns, results);
    }
    return results;
  }

  /**
   * Batch evaluate all eligible tests for input set of urns.
   *
   * @param urns Entity urns to evaluate
   * @param shouldPush Whether or not to push the test results into DataHub
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> batchEvaluateTestsForEntities(List<Urn> urns, boolean shouldPush) {
    Map<String, List<Urn>> urnsPerEntityType = urns.stream().collect(Collectors.groupingBy(Urn::getEntityType));
    Map<Urn, TestResults> finalTestResults = new HashMap<>();
    for (String entityType : urnsPerEntityType.keySet()) {
      List<TestDefinition> testsEligibleForEntityType =
          _testPerEntityTypeCache.getOrDefault(entityType, Collections.emptyList());
      if (testsEligibleForEntityType.isEmpty()) {
        continue;
      }
      Map<Urn, TestResults> resultsForEntityType = batchEvaluateTests(urns, testsEligibleForEntityType);
      finalTestResults.putAll(resultsForEntityType);
    }

    if (shouldPush) {
      finalTestResults.forEach(this::ingestResults);
    }

    return finalTestResults;
  }

  /**
   * Batch evaluate all eligible tests for input set of urns
   *
   * @param urns Entity urns to evaluate
   * @param testUrns Tests to evaluate
   * @param shouldPush Whether or not to push the test results into DataHub
   * @return Test results per entity urn
   */
  public Map<Urn, TestResults> batchEvaluateTests(List<Urn> urns, List<Urn> testUrns, boolean shouldPush) {
    Map<Urn, TestResults> resultsPerUrn = batchEvaluateTests(urns, fetchDefinitions(testUrns));

    if (shouldPush) {
      resultsPerUrn.forEach((urn, results) -> ingestPartialResults(urn, testUrns, results));
    }
    return resultsPerUrn;
  }

  private List<TestDefinition> fetchDefinitions(List<Urn> testUrns) {
    List<TestDefinition> tests = new ArrayList<>(testUrns.size());
    for (Urn testUrn : testUrns) {
      if (_testCache.containsKey(testUrn)) {
        tests.add(_testCache.get(testUrn));
      } else {
        log.info("Test {} does not exist: Skipping", testUrn);
      }
    }
    return tests;
  }

  // Get eligible tests for the given entity based on the targeting rules
  private List<TestDefinition> getEligibleTests(Urn urn, List<TestDefinition> tests) {
    // First batch evaluate all queries in the targeting rules
    // Always make sure we batch queries together as much as possible to reduce calls
    Map<TestQuery, TestQueryResponse> targetingRulesQueryResponses =
        batchQuery(ImmutableList.of(urn), getPredicatesFromTargetingRules(tests)).getOrDefault(urn,
            Collections.emptyMap());
    // Based on the query evaluation result, find the list of tests that are eligible for the given urn
    return tests.stream()
        .filter(test -> evaluateTargetingRules(urn, targetingRulesQueryResponses, test))
        .collect(Collectors.toList());
  }

  @WithSpan
  private TestResults evaluateTests(Urn urn, List<TestDefinition> tests) {
    if (tests.isEmpty()) {
      return EMPTY_RESULTS;
    }
    List<TestDefinition> eligibleTests = getEligibleTests(urn, tests);
    TestResults result = new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray());

    // Batch evaluate all queries in the main rules
    Map<TestQuery, TestQueryResponse> mainRulesQueryResponses = batchQuery(ImmutableList.of(urn),
        eligibleTests.stream().map(TestDefinition::getRule).collect(Collectors.toList())).getOrDefault(urn,
        Collections.emptyMap());
    // Evaluate whether each test passes using the query evaluation result above (no service calls. pure computation)
    for (TestDefinition eligibleTest : eligibleTests) {
      if (_testPredicateEvaluator.evaluate(eligibleTest.getRule(), mainRulesQueryResponses)) {
        result.getPassing().add(new TestResult().setTest(eligibleTest.getTestUrn()).setType(TestResultType.SUCCESS));
      } else {
        result.getFailing().add(new TestResult().setTest(eligibleTest.getTestUrn()).setType(TestResultType.FAILURE));
      }
    }
    return result;
  }

  // For each test, find the list of eligible entities among the input urns based on the targeting rules
  private Map<TestDefinition, List<Urn>> getEligibleEntitiesPerTest(List<Urn> urns, List<TestDefinition> tests) {
    // First batch evaluate all queries in the targeting rules
    // Always make sure we batch queries together as much as possible to reduce calls
    Map<Urn, Map<TestQuery, TestQueryResponse>> targetingRulesQueryResponses =
        batchQuery(urns, getPredicatesFromTargetingRules(tests));
    Map<TestDefinition, List<Urn>> eligibleTestsPerEntity = new HashMap<>();
    // Using the query evaluation result, find the set of eligible tests per entity.
    // Reverse map to get the list of entities eligible for each test
    for (Urn urn : urns) {
      Map<TestQuery, TestQueryResponse> queryResponseForUrn =
          targetingRulesQueryResponses.getOrDefault(urn, Collections.emptyMap());
      tests.stream().filter(test -> evaluateTargetingRules(urn, queryResponseForUrn, test)).forEach(test -> {
        if (!eligibleTestsPerEntity.containsKey(test)) {
          eligibleTestsPerEntity.put(test, new ArrayList<>());
        }
        eligibleTestsPerEntity.get(test).add(urn);
      });
    }
    return eligibleTestsPerEntity;
  }

  @WithSpan
  private Map<Urn, TestResults> batchEvaluateTests(List<Urn> urns, List<TestDefinition> tests) {
    if (tests.isEmpty()) {
      return urns.stream().collect(Collectors.toMap(Function.identity(), urn -> EMPTY_RESULTS));
    }
    Map<TestDefinition, List<Urn>> eligibleTestsPerEntity = getEligibleEntitiesPerTest(urns, tests);
    Map<Urn, TestResults> finalTestResults = urns.stream()
        .collect(Collectors.toMap(Function.identity(),
            urn -> new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray())));
    // For each test with eligible entities
    for (TestDefinition testDefinition : eligibleTestsPerEntity.keySet()) {
      List<Urn> urnsToTest = eligibleTestsPerEntity.get(testDefinition);

      // Batch evaluate all queries in the main rules for all eligible entities
      Map<Urn, Map<TestQuery, TestQueryResponse>> mainRulesQueryResponses =
          batchQuery(urnsToTest, ImmutableList.of(testDefinition.getRule()));
      // For each entity, evaluate whether it passes the test using the query evaluation results
      for (Urn urn : urnsToTest) {
        if (_testPredicateEvaluator.evaluate(testDefinition.getRule(),
            mainRulesQueryResponses.getOrDefault(urn, Collections.emptyMap()))) {
          finalTestResults.get(urn)
              .getPassing()
              .add(new TestResult().setTest(testDefinition.getTestUrn()).setType(TestResultType.SUCCESS));
        } else {
          finalTestResults.get(urn)
              .getFailing()
              .add(new TestResult().setTest(testDefinition.getTestUrn()).setType(TestResultType.FAILURE));
        }
      }
    }
    return finalTestResults;
  }

  /**
   * Evaluate whether the entity passes the targeting rules in the test.
   * Note, we batch evaluate queries before calling this function, and the results are inputted as arguments
   * This function purely uses the query evaluation results and checks whether it passes the targeting rules or not
   *
   * @param urn Entity urn in question
   * @param targetingRulesQueryResponses Batch query evaluation results.
   *                                     Contains the result of all queries in the targeting rules
   * @param test Test that we are trying to evaluate
   * @return Whether or not the entity passes the targeting rules of the test
   */
  private boolean evaluateTargetingRules(Urn urn, Map<TestQuery, TestQueryResponse> targetingRulesQueryResponses,
      TestDefinition test) {
    if (!test.getTarget().getEntityTypes().contains(urn.getEntityType())) {
      return false;
    }
    Optional<TestPredicate> targetingRule = test.getTarget().getTargetingRules();
    // Apply to all entities of given type
    if (!targetingRule.isPresent()) {
      return true;
    }
    return _testPredicateEvaluator.evaluate(targetingRule.get(), targetingRulesQueryResponses);
  }

  private List<TestPredicate> getPredicatesFromTargetingRules(List<TestDefinition> tests) {
    return tests.stream()
        .map(test -> test.getTarget().getTargetingRules())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @WithSpan
  private Map<Urn, Map<TestQuery, TestQueryResponse>> batchQuery(List<Urn> urns, List<TestPredicate> rules) {
    if (rules.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<TestQuery> requiredQueries = rules.stream()
        .flatMap(rule -> _testPredicateEvaluator.getRequiredQueries(rule).stream())
        .collect(Collectors.toSet());
    return _queryEngine.batchEvaluateQueries(new HashSet<>(urns), requiredQueries);
  }

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
    TestResults prevResults =
        new TestResults(entityResponse.getAspects().get(Constants.TEST_RESULTS_ASPECT_NAME).getValue().data());
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

  private void ingestResults(Urn urn, TestResults results) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.TEST_RESULTS_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(results));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(proposal,
        new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));
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
    private final TestDefinitionProvider _testDefinitionProvider;
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
            _testDefinitionProvider.deserialize(test.getUrn(), test.getTestInfo().getDefinition().getJson());
      } catch (TestDefinitionParsingException e) {
        log.error("Issue while deserializing test definition {}", test.getTestInfo().getDefinition().getJson(), e);
        return;
      }
      testCache.put(test.getUrn(), testDefinition);
      for (String entityType : testDefinition.getTarget().getEntityTypes()) {
        List<TestDefinition> existingTestsForEntityType = cache.getOrDefault(entityType, new ArrayList<>());
        existingTestsForEntityType.add(testDefinition);
        cache.put(entityType, existingTestsForEntityType);
      }
    }
  }
}
