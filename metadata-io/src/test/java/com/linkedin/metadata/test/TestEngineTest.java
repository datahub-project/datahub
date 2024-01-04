package com.linkedin.metadata.test;

import static com.linkedin.metadata.test.TestDefinitionParserTest.loadTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestResults;
import com.linkedin.test.TestStatus;
import java.util.List;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TestEngineTest {

  final EntityService mockEntityService = Mockito.mock(EntityService.class);
  final TestFetcher mockTestFetcher = Mockito.mock(TestFetcher.class);
  final QueryEngine mockQueryEngine = Mockito.mock(QueryEngine.class);
  final ActionApplier mockActionApplier = Mockito.mock(ActionApplier.class);
  final PredicateEvaluator spyPredicateEvaluator = Mockito.spy(PredicateEvaluator.getInstance());

  @Test
  public void testInitialization() throws Exception {
    List<TestFetcher.Test> tests = List.of(buildSimpleTest("123"));
    TestEngine testEngine = buildTestEngine(tests);
    assertNotNull(testEngine);

    // Defaults to no match
    TestResults result =
        testEngine.evaluateTests(
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:datahub,DataPlatform,PROD)"),
            TestEngine.EvaluationMode.EVALUATE_ONLY);
    assertTrue(result.hasFailing());

    // Pretend match
    doReturn(true).when(spyPredicateEvaluator).evaluatePredicate(any(), any());

    result =
        testEngine.evaluateTests(
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
            Urn.createFromString("urn:li:chart:(looker,dashboard_elements.17)"),
            Set.of(tests.get(0).getUrn()),
            TestEngine.EvaluationMode.DEFAULT);

    // test is successful if no exception and has no results
    assertTrue(result.getFailing().isEmpty());
    assertTrue(result.getPassing().isEmpty());
  }

  /** on: types: - dataset rules: and: - property: status.removed operator: is_true */
  private TestFetcher.Test buildSimpleTest(String id) throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:test:" + id);

    TestInfo testInfo = new TestInfo();
    testInfo.setName("Test " + id);
    testInfo.setStatus(new TestStatus().setMode(TestMode.ACTIVE));

    TestDefinition testDefinition = new TestDefinition();
    testDefinition.setJson(loadTest("test/valid_testengine_simple.yaml"));
    testDefinition.setType(TestDefinitionType.JSON);
    testInfo.setDefinition(testDefinition);

    return new TestFetcher.Test(testUrn, testInfo);
  }

  private TestEngine buildTestEngine(List<TestFetcher.Test> withTests) throws Exception {
    Mockito.reset(
        mockEntityService,
        mockTestFetcher,
        mockQueryEngine,
        mockActionApplier,
        spyPredicateEvaluator);

    when(mockTestFetcher.fetch(anyInt(), anyInt()))
        .thenReturn(new TestFetcher.TestFetchResult(withTests, withTests.size()));

    final EntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            TestEngineTest.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    when(mockEntityService.getEntityRegistry()).thenReturn(entityRegistry);

    return new TestEngine(
        mockEntityService,
        mockTestFetcher,
        new TestDefinitionParser(spyPredicateEvaluator),
        mockQueryEngine,
        spyPredicateEvaluator,
        mockActionApplier,
        0,
        0);
  }
}
