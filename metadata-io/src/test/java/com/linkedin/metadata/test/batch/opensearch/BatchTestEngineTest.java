package com.linkedin.metadata.test.batch.opensearch;

import static com.linkedin.metadata.test.TestDefinitionParserTest.loadTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.reflect.ClassPath;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.config.ElasticSearchTestExecutorConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.TestsHookConfiguration;
import com.linkedin.metadata.config.TestsHookExecutionLimitConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import com.linkedin.metadata.service.TagServiceAsync;
import com.linkedin.metadata.test.BatchTestEngine;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.action.tag.AddTagsAction;
import com.linkedin.metadata.test.action.tag.RemoveTagsAction;
import com.linkedin.metadata.test.batch.BatchTestEngineEnvConfig;
import com.linkedin.metadata.test.batch.BatchTestEngineExecConfig;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.EntityUrnTypeEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.QueryVersionedAspectEvaluator;
import com.linkedin.metadata.test.query.StructuredPropertyEvaluator;
import com.linkedin.metadata.test.query.SystemAspectEvaluator;
import com.linkedin.metadata.test.query.schemafield.SchemaFieldEvaluator;
import com.linkedin.metadata.test.query.virtualFields.VirtualFieldsQueryEvaluator;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.fixtures.search.SampleDataFixtureConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Designed to test metadata test batch mode with opensearch scrolling */
@Import({
  OpenSearchSuite.class,
  SampleDataFixtureConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class BatchTestEngineTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("longTailEntitySearchService")
  protected EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("longTailOperationContext")
  protected OperationContext operationContext;

  @MockBean private SystemEntityClient mockEntityClient;
  @MockBean private TestFetcher mockTestFetcher;
  @MockBean private EntityService<?> mockEntityService;
  @MockBean private TimeseriesAspectService mockTimeseriesAspectService;
  @MockBean private OpenApiClient mockOpenApiClient;

  private TestEngine testEngine;

  @BeforeClass
  public void beforeTest() throws Exception {
    testEngine = buildTestEngine(loadTests());
  }

  @AfterClass
  public void afterTest() {
    verifyNoInteractions(mockOpenApiClient);
    testEngine.forceClose();
  }

  @Test
  public void initTest() {
    assertNotNull(entitySearchService);
    assertNotNull(testEngine);
  }

  @Test
  public void batchTestEngineTest() throws RemoteInvocationException, InterruptedException {
    BatchTestEngine batchTestEngine =
        new BatchTestEngine(operationContext, mockEntityClient, entitySearchService, testEngine);

    // Configure for minimum testing of parallelism and pagination
    BatchTestEngineEnvConfig envConfig =
        BatchTestEngineEnvConfig.builder()
            .evaluationMode(TestEngine.EvaluationMode.DEFAULT)
            .executorNumThreads(2)
            .executorQueueSize(2)
            .build();
    BatchTestEngineExecConfig execConfig =
        BatchTestEngineExecConfig.builder()
            .batchSize(100)
            .batchDelayMs(0)
            .runId("test-run")
            .logger(s -> {})
            .build();

    batchTestEngine.execute(envConfig, execConfig);
    testEngine.close();

    ArgumentCaptor<List<MetadataChangeProposal>> actionMCPCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockEntityClient, atLeastOnce())
        .batchIngestProposals(eq(operationContext), actionMCPCaptor.capture(), eq(true));

    ArgumentCaptor<AspectsBatchImpl> testResultsCaptor =
        ArgumentCaptor.forClass(AspectsBatchImpl.class);
    verify(mockEntityService, atLeastOnce())
        .ingestProposal(eq(operationContext), testResultsCaptor.capture(), eq(true));

    List<List<MetadataChangeProposal>> actualTagActionMCPs = actionMCPCaptor.getAllValues();
    Map<ChangeType, Map<String, Map<String, Long>>> tagActionBreakdown =
        breakdownMCPs(actualTagActionMCPs.stream().flatMap(List::stream));

    final long totalDatasetCount = 1898;
    final long totalChartCount = 223;
    final long totalDashboardCount = 43;

    // 1 patch to either add/remove tag for each type since even if the test fails an action is
    // performed
    // 1 dataset tests has no actions
    assertEquals(
        tagActionBreakdown,
        Map.of(
            ChangeType.PATCH,
            Map.of(
                "dataset",
                Map.of("globalTags", totalDatasetCount),
                "chart",
                Map.of("globalTags", totalChartCount),
                "dashboard",
                Map.of("globalTags", totalDashboardCount))));

    List<AspectsBatchImpl> actualAspectBatch = testResultsCaptor.getAllValues();
    Map<ChangeType, Map<String, Map<String, Long>>> testResultsBreakdown =
        breakdownMCPs(
            actualAspectBatch.stream()
                .flatMap(
                    batch -> batch.getMCPItems().stream().map(MCPItem::getMetadataChangeProposal)));

    // should collapse multiple tests into 1 upsert for the combined results for all tests
    assertEquals(
        testResultsBreakdown,
        Map.of(
            ChangeType.UPSERT,
            Map.of(
                "dataset",
                Map.of("testResults", totalDatasetCount),
                "chart",
                Map.of("testResults", totalChartCount),
                "dashboard",
                Map.of("testResults", totalDashboardCount))));
  }

  private TestEngine buildTestEngine(List<TestFetcher.Test> withTests) throws Exception {
    Mockito.reset(
        mockEntityService,
        mockTestFetcher,
        mockEntityClient,
        mockTimeseriesAspectService,
        mockEntityClient);

    PredicateEvaluator predicateEvaluator = PredicateEvaluator.getInstance();

    when(mockTestFetcher.fetch(any(), anyInt(), anyInt()))
        .thenReturn(new TestFetcher.TestFetchResult(withTests, withTests.size()));
    when(mockTestFetcher.fetchOne(any(), any()))
        .thenReturn(new TestFetcher.TestFetchResult(withTests, withTests.size()));

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

    ExecutorService actionsExecutorService =
        new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.CallerRunsPolicy());

    return new TestEngine(
        operationContext,
        testsConfiguration,
        mockEntityService,
        entitySearchService,
        mockTimeseriesAspectService,
        mockTestFetcher,
        new TestDefinitionParser(predicateEvaluator),
        queryEngine(),
        predicateEvaluator,
        actionApplier(),
        actionsExecutorService);
  }

  private QueryEngine queryEngine() {
    final EntityUrnTypeEvaluator urnTypeEvaluator = new EntityUrnTypeEvaluator();
    final QueryVersionedAspectEvaluator queryVersionedAspectEvaluator =
        new QueryVersionedAspectEvaluator(operationContext.getEntityRegistry(), mockEntityService);
    final SystemAspectEvaluator systemAspectEvaluator =
        new SystemAspectEvaluator(mockEntityService);
    final StructuredPropertyEvaluator structuredPropertyEvaluator =
        new StructuredPropertyEvaluator(mockEntityService);
    final SchemaFieldEvaluator schemaFieldEvaluator = new SchemaFieldEvaluator(mockEntityService);
    final VirtualFieldsQueryEvaluator virtualFieldsQueryEvaluator =
        new VirtualFieldsQueryEvaluator();
    return new QueryEngine(
        ImmutableList.of(
            urnTypeEvaluator,
            queryVersionedAspectEvaluator,
            systemAspectEvaluator,
            structuredPropertyEvaluator,
            schemaFieldEvaluator,
            virtualFieldsQueryEvaluator));
  }

  private ActionApplier actionApplier() {
    ObjectMapper objectMapper = new ObjectMapper();
    TagServiceAsync tagService =
        new TagServiceAsync(mockEntityClient, mockOpenApiClient, objectMapper);
    List<Action> appliers = new ArrayList<>();
    appliers.add(new AddTagsAction(tagService));
    appliers.add(new RemoveTagsAction(tagService));
    return new ActionApplier(appliers);
  }

  private List<TestFetcher.Test> loadTests() throws Exception {
    return ClassPath.from(this.getClass().getClassLoader()).getResources().stream()
        .filter(resource -> resource.getResourceName().startsWith("test/batch_test"))
        .filter(
            resource -> {
              String name = resource.getResourceName().toLowerCase();
              return name.endsWith(".yaml") || name.endsWith(".yml");
            })
        .map(ClassPath.ResourceInfo::getResourceName)
        .map(
            resourceName -> {
              try {
                String baseFilename = resourceName.substring(resourceName.lastIndexOf('/') + 1);
                String nameWithoutExtension = Files.getNameWithoutExtension(baseFilename);

                Urn testUrn = UrnUtils.getUrn("urn:li:test:" + nameWithoutExtension);

                TestInfo testInfo = new TestInfo();
                testInfo.setName("Test " + nameWithoutExtension);
                testInfo.setStatus(new TestStatus().setMode(TestMode.ACTIVE));

                TestDefinition testDefinition = new TestDefinition();
                testDefinition.setJson(loadTest(resourceName));

                testDefinition.setType(TestDefinitionType.JSON);
                testInfo.setDefinition(testDefinition);

                return new TestFetcher.Test(testUrn, testInfo);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  private Map<ChangeType, Map<String, Map<String, Long>>> breakdownMCPs(
      Stream<MetadataChangeProposal> mcpStream) {
    Map<ChangeType, Map<String, Map<String, AtomicLong>>> atomicBreakdown =
        new ConcurrentHashMap<>();

    mcpStream
        .parallel()
        .forEach(
            mcp ->
                atomicBreakdown
                    .computeIfAbsent(mcp.getChangeType(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(mcp.getEntityType(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(mcp.getAspectName(), k -> new AtomicLong())
                    .incrementAndGet());

    // Convert the nested structure from AtomicLong to Long
    return atomicBreakdown.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                innerEntry ->
                                    innerEntry.getValue().entrySet().stream()
                                        .collect(
                                            Collectors.toMap(
                                                Map.Entry::getKey,
                                                leafEntry -> leafEntry.getValue().get(),
                                                (v1, v2) -> v2,
                                                HashMap::new)),
                                (v1, v2) -> v2,
                                HashMap::new)),
                (v1, v2) -> v2,
                HashMap::new));
  }
}
