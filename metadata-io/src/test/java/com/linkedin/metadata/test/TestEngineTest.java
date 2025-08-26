package com.linkedin.metadata.test;

import static com.linkedin.metadata.test.TestConstants.*;
import static com.linkedin.metadata.test.TestDefinitionParserTest.loadTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.ElasticSearchTestExecutorConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.TestsHookConfiguration;
import com.linkedin.metadata.config.TestsHookExecutionLimitConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.exception.SelectionTooLargeException;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import com.linkedin.test.TestStatus;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TestEngineTest {

  final EntityService mockEntityService = Mockito.mock(EntityService.class);

  final TimeseriesAspectService mockTimeseriesAspectService =
      Mockito.mock(TimeseriesAspectService.class);
  final TestFetcher mockTestFetcher = Mockito.mock(TestFetcher.class);
  final QueryEngine mockQueryEngine = Mockito.mock(QueryEngine.class);
  final ActionApplier mockActionApplier = Mockito.mock(ActionApplier.class);
  final PredicateEvaluator spyPredicateEvaluator = Mockito.spy(PredicateEvaluator.getInstance());
  final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);

  @Test
  public void testInitialization() throws Exception {
    List<TestFetcher.Test> tests = List.of(buildSimpleTest("123"));
    TestEngine testEngine = buildTestEngine(tests);
    assertNotNull(testEngine);

    // Defaults to no match
    TestResults result =
        testEngine.evaluateTests(
            mock(OperationContext.class),
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:datahub,DataPlatform,PROD)"),
            TestEngine.EvaluationMode.EVALUATE_ONLY);
    assertTrue(result.hasFailing());

    // Pretend match
    doReturn(true).when(spyPredicateEvaluator).evaluatePredicate(any(), any());

    result =
        testEngine.evaluateTests(
            mock(OperationContext.class),
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:datahub,DataPlatform,PROD)"),
            TestEngine.EvaluationMode.EVALUATE_ONLY);
    assertTrue(result.hasPassing());
  }

  @Test
  public void testEmptyPatch() throws Exception {
    List<TestFetcher.Test> tests = List.of(buildSimpleTest("123"));
    TestEngine testEngine = buildTestEngine(tests);

    // doesn't apply to charts
    TestResults result =
        testEngine.evaluateTestUrns(
            mock(OperationContext.class),
            Urn.createFromString("urn:li:chart:(looker,dashboard_elements.17)"),
            Set.of(tests.get(0).getUrn()),
            TestEngine.EvaluationMode.DEFAULT);

    // test is successful if no exception and has no results
    assertTrue(result.getFailing().isEmpty());
    assertTrue(result.getPassing().isEmpty());
  }

  @Test
  public void testSingleEvalLimits() throws Exception {
    TestFetcher.Test testDefinition = buildSimpleTest("123");
    List<TestFetcher.Test> tests = List.of(testDefinition);
    TestEngine testEngine = buildTestEngine(tests);
    assertNotNull(testEngine);
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(10);
    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    SelectionTooLargeException ex = null;
    try {
      Map<Urn, TestResults> result =
          testEngine.evaluateSingleTest(
              mock(OperationContext.class),
              testDefinition.getUrn(),
              TestEngine.EvaluationMode.DEFAULT);
    } catch (SelectionTooLargeException e) {
      ex = e;
    }
    assertNotNull(ex);

    testDefinition = buildNonESCompatibleTest("456");
    tests = List.of(testDefinition);
    testEngine = buildTestEngine(tests);
    searchResult.setNumEntities(1);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,data,PROD)"));
    searchEntities.add(searchEntity);
    searchResult.setEntities(searchEntities);
    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);
    ex = null;
    try {
      Map<Urn, TestResults> result =
          testEngine.evaluateSingleTest(
              mock(OperationContext.class),
              testDefinition.getUrn(),
              TestEngine.EvaluationMode.DEFAULT);
    } catch (SelectionTooLargeException e) {
      ex = e;
    }
    assertNotNull(ex);
  }

  @Test
  public void testSingleEvalNoQueryProvided() throws Exception {
    TestFetcher.Test testDefinition = buildSimpleTest("123");
    List<TestFetcher.Test> tests = List.of(testDefinition);
    TestEngine testEngine = buildTestEngine(tests);
    assertNotNull(testEngine);
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,data,PROD)"));
    searchEntities.add(searchEntity);
    searchResult.setEntities(searchEntities);
    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // run method
    testEngine.evaluateSingleTest(
        mock(OperationContext.class), testDefinition.getUrn(), TestEngine.EvaluationMode.DEFAULT);

    // verify search is called with correct default query
    Mockito.verify(mockSearchService, Mockito.times(2))
        .predicateSearch(any(), any(), eq("*"), any(), any(), anyInt(), anyInt(), any());
  }

  @Test
  public void testSingleEvalQueryProvided() throws Exception {
    TestFetcher.Test testDefinition = buildSimpleTest("123", "test query");
    List<TestFetcher.Test> tests = List.of(testDefinition);
    TestEngine testEngine = buildTestEngine(tests);
    assertNotNull(testEngine);
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,data,PROD)"));
    searchEntities.add(searchEntity);
    searchResult.setEntities(searchEntities);
    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // run method
    testEngine.evaluateSingleTest(
        mock(OperationContext.class), testDefinition.getUrn(), TestEngine.EvaluationMode.DEFAULT);

    // verify search is called with correct provided query
    Mockito.verify(mockSearchService, Mockito.times(2))
        .predicateSearch(any(), any(), eq("test query"), any(), any(), anyInt(), anyInt(), any());
  }

  @Test
  public void testExplain() throws Exception {
    TestFetcher.Test test = buildSimpleTest("123");
    List<TestFetcher.Test> tests = List.of(test);
    TestEngine testEngine = buildTestEngine(tests);
    com.linkedin.metadata.test.definition.TestDefinition testDefinition =
        buildSimpleTestDefinition(testEngine);

    assertNotNull(testEngine);
    List<String> explainSelect =
        testEngine.getElasticSearchTestExecutor().explainSelect(testDefinition);
    assertEquals(explainSelect.size(), 1);
    List<String> explainEvaluate =
        testEngine.getElasticSearchTestExecutor().explainEvaluate(testDefinition);
    assertEquals(explainEvaluate.size(), 1);

    test = buildNonESCompatibleTest("456");
    tests = List.of(test);
    testEngine = buildTestEngine(tests);
    testDefinition = buildNonESCompatibleTestDefinition(testEngine);
    explainSelect = testEngine.getElasticSearchTestExecutor().explainSelect(testDefinition);
    assertEquals(explainSelect.size(), 1);
    explainEvaluate = testEngine.getElasticSearchTestExecutor().explainEvaluate(testDefinition);
    assertEquals(explainEvaluate.size(), 2);
  }

  @Test
  public void testBatchTestResultAggregation() throws Exception {
    TestFetcher.Test test = buildSimpleTest("123");
    TestEngine testEngine = buildTestEngine(List.of(test));
    Urn dataset1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,dataset1,PROD)");
    Urn dataset2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,dataset2,PROD)");

    // Mock search results to return multiple batches
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(2);
    SearchEntityArray searchEntities = new SearchEntityArray();
    searchEntities.add(new SearchEntity().setEntity(dataset1));
    searchEntities.add(new SearchEntity().setEntity(dataset2));
    searchResult.setEntities(searchEntities);

    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Test evaluating multiple entities
    Map<Urn, TestResults> results =
        testEngine.evaluateSingleTest(
            mock(OperationContext.class), test.getUrn(), TestEngine.EvaluationMode.DEFAULT);

    assertEquals(results.size(), 2);
    assertTrue(results.containsKey(dataset1));
    assertTrue(results.containsKey(dataset2));

    results.forEach(
        (testUrn, result) -> {
          assertTrue(result.getFailing().isEmpty());
          assertEquals(result.getPassing().get(0).getTest(), test.getUrn());
          assertEquals(result.getPassing().get(0).getType(), TestResultType.SUCCESS);
          assertEquals(
              result.getPassing().get(0).getTestDefinitionMd5(),
              "d62adc9af36875ffbef60dff62d0110b");
        });
  }

  @Test
  public void testCacheInvalidationAndRefresh() throws Exception {
    TestFetcher.Test test = buildSimpleTest("123");
    TestEngine testEngine = buildTestEngine(List.of(test));

    // Initial cache state
    assertEquals(testEngine.getEntityTypesToEvaluate().size(), 1);

    // Mock fetcher to return different results
    TestFetcher.Test newTest = buildSimpleTest("456");
    when(mockTestFetcher.fetch(any(), anyInt(), anyInt()))
        .thenReturn(new TestFetcher.TestFetchResult(List.of(newTest), 1));

    // Test cache invalidation
    testEngine.invalidateCache();
    testEngine.loadTests();

    // Verify cache was updated
    verify(mockTestFetcher, times(2)).fetch(any(), anyInt(), anyInt());
  }

  @Test
  public void testDifferentEvaluationModes() throws Exception {
    TestFetcher.Test test = buildSimpleTest("123");
    TestEngine testEngine = buildTestEngine(List.of(test));

    // Mock search service
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,data,PROD)"));
    searchEntities.add(searchEntity);
    searchResult.setEntities(searchEntities);
    when(mockSearchService.predicateSearch(
            any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Mock query responses for success case
    Map<TestQuery, TestQueryResponse> queryResponses = new HashMap<>();
    when(mockQueryEngine.batchEvaluateQueries(any(), any(), any()))
        .thenReturn(
            Collections.singletonMap(
                UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,data,PROD)"),
                queryResponses));

    // Test EVALUATE_ONLY mode
    Map<Urn, TestResults> results =
        testEngine.evaluateSingleTest(
            mock(OperationContext.class), test.getUrn(), TestEngine.EvaluationMode.EVALUATE_ONLY);
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());

    // Test SYNC mode
    results =
        testEngine.evaluateSingleTest(
            mock(OperationContext.class), test.getUrn(), TestEngine.EvaluationMode.SYNC);
    verify(mockEntityService, atLeastOnce()).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testShouldWriteAssetResults() {
    // Test with null source
    TestInfo testInfo = new TestInfo();
    assertTrue(TestEngine.shouldWriteAssetResults(testInfo));

    // Test with BULK_FORM_SUBMISSION source
    TestInfo bulkFormTestInfo = new TestInfo();
    bulkFormTestInfo.setSource(new TestSource().setType(TestSourceType.BULK_FORM_SUBMISSION));
    assertFalse(TestEngine.shouldWriteAssetResults(bulkFormTestInfo));

    // Test with FORMS source
    TestInfo formsTestInfo = new TestInfo();
    formsTestInfo.setSource(new TestSource().setType(TestSourceType.FORMS));
    assertFalse(TestEngine.shouldWriteAssetResults(formsTestInfo));

    // Test with FORM_PROMPT source
    TestInfo formPromptTestInfo = new TestInfo();
    formPromptTestInfo.setSource(new TestSource().setType(TestSourceType.FORM_PROMPT));
    assertFalse(TestEngine.shouldWriteAssetResults(formPromptTestInfo));

    // Test with other source type
    TestInfo otherTestInfo = new TestInfo();
    otherTestInfo.setSource(new TestSource().setType(TestSourceType.$UNKNOWN));
    assertTrue(TestEngine.shouldWriteAssetResults(otherTestInfo));

    // Test with no source
    TestInfo noSourceTestInfo = new TestInfo();
    assertTrue(TestEngine.shouldWriteAssetResults(noSourceTestInfo));
  }

  /** on: types: - dataset rules: and: - property: status.removed operator: is_true */
  private TestFetcher.Test buildSimpleTest(String id) throws Exception {
    return buildSimpleTest(id, null);
  }

  /** on: types: - dataset rules: and: - property: status.removed operator: is_true */
  private TestFetcher.Test buildSimpleTest(String id, @Nullable String query) throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:test:" + id);

    TestInfo testInfo = new TestInfo();
    testInfo.setName("Test " + id);
    testInfo.setStatus(new TestStatus().setMode(TestMode.ACTIVE));

    TestDefinition testDefinition = new TestDefinition();
    testDefinition.setJson(loadTest("test/valid_testengine_simple.yaml"));
    testDefinition.setType(TestDefinitionType.JSON);
    if (query != null) {
      testDefinition.setOnQuery(query);
    }
    testInfo.setDefinition(testDefinition);

    return new TestFetcher.Test(testUrn, testInfo);
  }

  private com.linkedin.metadata.test.definition.TestDefinition buildSimpleTestDefinition(
      TestEngine testEngine) throws Exception {
    return testEngine
        .getParser()
        .deserialize(DUMMY_TEST_URN, loadTest("test/valid_testengine_simple.yaml"));
  }

  private TestFetcher.Test buildNonESCompatibleTest(String id) throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:test:" + id);

    TestInfo testInfo = new TestInfo();
    testInfo.setName("Test " + id);
    testInfo.setStatus(new TestStatus().setMode(TestMode.ACTIVE));

    TestDefinition testDefinition = new TestDefinition();
    testDefinition.setJson(loadTest("test/valid_non_elastic_test.yaml"));
    testDefinition.setType(TestDefinitionType.JSON);
    testInfo.setDefinition(testDefinition);

    return new TestFetcher.Test(testUrn, testInfo);
  }

  private com.linkedin.metadata.test.definition.TestDefinition buildNonESCompatibleTestDefinition(
      TestEngine testEngine) throws Exception {
    return testEngine
        .getParser()
        .deserialize(DUMMY_TEST_URN, loadTest("test/valid_non_elastic_test.yaml"));
  }

  private TestEngine buildTestEngine(List<TestFetcher.Test> withTests) throws Exception {
    Mockito.reset(
        mockEntityService,
        mockTestFetcher,
        mockQueryEngine,
        mockActionApplier,
        spyPredicateEvaluator,
        mockSearchService);

    when(mockTestFetcher.fetch(any(), anyInt(), anyInt()))
        .thenReturn(new TestFetcher.TestFetchResult(withTests, withTests.size()));
    when(mockTestFetcher.fetchOne(any(), any()))
        .thenReturn(new TestFetcher.TestFetchResult(withTests, withTests.size()));
    when(mockQueryEngine.validateQuery(any(TestQuery.class), anyList()))
        .thenReturn(ValidationResult.validResult());

    final EntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            TestEngineTest.class.getClassLoader().getResourceAsStream("entity-registry.yml"));

    TestsHookExecutionLimitConfiguration testsHookExecutionLimitConfiguration =
        new TestsHookExecutionLimitConfiguration();
    testsHookExecutionLimitConfiguration.setDefaultExecutor(1);
    testsHookExecutionLimitConfiguration.setElasticSearchExecutor(10);

    TestsConfiguration testsConfiguration = new TestsConfiguration();
    testsConfiguration.setEnabled(true);
    testsConfiguration.setCacheRefreshDelayIntervalSecs(0);
    testsConfiguration.setCacheRefreshIntervalSecs(0);
    testsConfiguration.setElasticSearchExecutor(new ElasticSearchTestExecutorConfiguration());
    testsConfiguration.getElasticSearchExecutor().setEnabled(true);
    testsConfiguration.setHook(new TestsHookConfiguration());
    testsConfiguration.getHook().setHookExecutionLimit(testsHookExecutionLimitConfiguration);

    return new TestEngine(
        TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry),
        testsConfiguration,
        mockEntityService,
        mockSearchService,
        mockTimeseriesAspectService,
        mockTestFetcher,
        new TestDefinitionParser(spyPredicateEvaluator),
        mockQueryEngine,
        spyPredicateEvaluator,
        mockActionApplier,
        mock(ExecutorService.class));
  }
}
