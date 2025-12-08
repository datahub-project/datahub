package com.linkedin.metadata.search;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticSearchServiceTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)");
  private static final String TEST_DOC_ID =
      URLEncoder.encode(TEST_URN.toString(), StandardCharsets.UTF_8);
  private static final int MAX_RUN_IDS_INDEXED = 25;

  @Mock private ESWriteDAO mockEsWriteDAO;

  private ElasticSearchService testInstance;
  private static final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private static final int DEFAULT_LIMIT = 100;
  private static final int MAX_LIMIT = 1000;
  private ElasticSearchService esSearchServiceLimited;
  private static final String ENTITY_NAME = "dataset";
  private static final String PATH = "/prod/kafka";
  private static final String INPUT = "test-input";
  private static final int START = 0;

  @Mock private ESBrowseDAO esBrowseDAO;
  @Mock private BrowseResultV2 browseResultV2;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    testInstance =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    // Setup search service configuration
    ResultsLimitConfig resultsLimitConfig =
        ResultsLimitConfig.builder().apiDefault(DEFAULT_LIMIT).max(MAX_LIMIT).strict(true).build();

    LimitConfig limitConfig = LimitConfig.builder().results(resultsLimitConfig).build();

    SearchServiceConfiguration searchServiceConfig =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    // Initialize ElasticSearchService
    esSearchServiceLimited =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            searchServiceConfig,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            esBrowseDAO,
            mock(ESWriteDAO.class));

    when(esBrowseDAO.browseV2(
            any(OperationContext.class),
            any(String.class),
            any(),
            nullable(Filter.class),
            any(),
            anyInt(),
            nullable(Integer.class)))
        .thenReturn(browseResultV2);
    when(esBrowseDAO.browseV2(
            any(OperationContext.class),
            anyList(),
            any(),
            nullable(Filter.class),
            any(),
            anyInt(),
            nullable(Integer.class)))
        .thenReturn(browseResultV2);
  }

  @Test
  public void testAppendRunId_ValidRunId() {
    String runId = "test-run-id";

    // Execute
    testInstance.appendRunId(opContext, TEST_URN, runId);

    // Capture and verify the script update parameters
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> scriptSourceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<String, Object>> scriptParamsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptSourceCaptor.capture(),
            scriptParamsCaptor.capture(),
            upsertCaptor.capture());

    // Verify script content
    String expectedScriptSource = ElasticSearchService.SCRIPT_SOURCE;
    assertEquals(expectedScriptSource, scriptSourceCaptor.getValue());

    // Verify script parameters
    Map<String, Object> expectedParams = new HashMap<>();
    expectedParams.put("runId", runId);
    expectedParams.put("maxRunIds", MAX_RUN_IDS_INDEXED);
    assertEquals(expectedParams, scriptParamsCaptor.getValue());

    // Verify upsert document
    Map<String, Object> capturedUpsert = upsertCaptor.getValue();
    assertEquals(Collections.singletonList(runId), capturedUpsert.get("runId"));
    assertEquals(TEST_URN.toString(), capturedUpsert.get("urn"));
  }

  @Test
  public void testAppendRunId_NullRunId() {
    // Execute with null runId
    testInstance.appendRunId(opContext, TEST_URN, null);

    // Verify the script update is still called with null handling
    ArgumentCaptor<String> scriptSourceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<String, Object>> scriptParamsCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptSourceCaptor.capture(),
            scriptParamsCaptor.capture(),
            upsertCaptor.capture());

    String expectedScriptSource = ElasticSearchService.SCRIPT_SOURCE;
    // Verify script content handles null
    assertEquals(expectedScriptSource, scriptSourceCaptor.getValue());
    Map<String, Object> expectedParams = new HashMap<>();
    expectedParams.put("runId", null);
    expectedParams.put("maxRunIds", MAX_RUN_IDS_INDEXED);
    assertEquals(expectedParams, scriptParamsCaptor.getValue());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRunId_NullUrn() {
    testInstance.appendRunId(opContext, null, "test-run-id");
  }

  @Test
  public void testAppendRunId_DualWriteToSemanticIndex() {
    // Setup: Create a mock that supports indexExists check
    ESWriteDAO mockEsWriteDAOWithIndex = mock(ESWriteDAO.class);
    when(mockEsWriteDAOWithIndex.indexExists(any(String.class))).thenReturn(true);

    ElasticSearchService serviceWithSemanticIndex =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            mock(ESBrowseDAO.class),
            mockEsWriteDAOWithIndex);

    String runId = "test-run-id-semantic";

    // Execute
    serviceWithSemanticIndex.appendRunId(opContext, TEST_URN, runId);

    // Verify V2 index update
    verify(mockEsWriteDAOWithIndex)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            eq(ElasticSearchService.SCRIPT_SOURCE),
            any(Map.class),
            any(Map.class));

    // Verify semantic index update
    String expectedSemanticIndexName =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameSemantic(TEST_URN.getEntityType());

    // Capture the arguments for semantic index update
    ArgumentCaptor<String> indexNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> docIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> scriptSourceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> scriptParamsCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map> upsertCaptor = ArgumentCaptor.forClass(Map.class);

    verify(mockEsWriteDAOWithIndex)
        .applyScriptUpdateByIndexName(
            indexNameCaptor.capture(),
            docIdCaptor.capture(),
            scriptSourceCaptor.capture(),
            scriptParamsCaptor.capture(),
            upsertCaptor.capture());

    // Verify the semantic index was updated correctly
    assertEquals(expectedSemanticIndexName, indexNameCaptor.getValue());
    assertEquals(TEST_DOC_ID, docIdCaptor.getValue());
    assertEquals(ElasticSearchService.SCRIPT_SOURCE, scriptSourceCaptor.getValue());
    assertEquals(runId, scriptParamsCaptor.getValue().get("runId"));
    assertEquals(MAX_RUN_IDS_INDEXED, scriptParamsCaptor.getValue().get("maxRunIds"));
  }

  @Test
  public void testAppendRunId_SkipsSemanticIndexWhenNotExists() {
    // Setup: Create a mock where semantic index does not exist
    ESWriteDAO mockEsWriteDAONoSemanticIndex = mock(ESWriteDAO.class);
    when(mockEsWriteDAONoSemanticIndex.indexExists(any(String.class))).thenReturn(false);

    ElasticSearchService serviceWithoutSemanticIndex =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            mock(ESBrowseDAO.class),
            mockEsWriteDAONoSemanticIndex);

    String runId = "test-run-id";

    // Execute
    serviceWithoutSemanticIndex.appendRunId(opContext, TEST_URN, runId);

    // Verify V2 index update was called
    verify(mockEsWriteDAONoSemanticIndex)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            eq(ElasticSearchService.SCRIPT_SOURCE),
            any(Map.class),
            any(Map.class));

    // Verify semantic index update was NOT called (since index doesn't exist)
    verify(mockEsWriteDAONoSemanticIndex, never())
        .applyScriptUpdateByIndexName(
            any(String.class),
            any(String.class),
            any(String.class),
            any(Map.class),
            any(Map.class));
  }

  @Test
  public void testRaw_WithValidUrns() {
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    testInstance =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockEsSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    // Create test data
    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset1,PROD)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset2,PROD)");
    Set<Urn> urns = Set.of(urn1, urn2);

    // Create mock search hits for each URN
    org.opensearch.search.SearchHit hit1 = mock(org.opensearch.search.SearchHit.class);
    Map<String, Object> sourceMap1 = Map.of("field1", "value1", "field2", 123);
    when(hit1.getSourceAsMap()).thenReturn(sourceMap1);

    org.opensearch.search.SearchHit hit2 = mock(org.opensearch.search.SearchHit.class);
    Map<String, Object> sourceMap2 = Map.of("field1", "value2", "field3", true);
    when(hit2.getSourceAsMap()).thenReturn(sourceMap2);

    // Create mock search results
    org.opensearch.search.SearchHits searchHits1 = mock(org.opensearch.search.SearchHits.class);
    when(searchHits1.getHits()).thenReturn(new org.opensearch.search.SearchHit[] {hit1});

    org.opensearch.search.SearchHits searchHits2 = mock(org.opensearch.search.SearchHits.class);
    when(searchHits2.getHits()).thenReturn(new org.opensearch.search.SearchHit[] {hit2});

    org.opensearch.action.search.SearchResponse response1 =
        mock(org.opensearch.action.search.SearchResponse.class);
    when(response1.getHits()).thenReturn(searchHits1);

    org.opensearch.action.search.SearchResponse response2 =
        mock(org.opensearch.action.search.SearchResponse.class);
    when(response2.getHits()).thenReturn(searchHits2);

    // Mock the rawEntity response from ESSearchDAO
    Map<Urn, org.opensearch.action.search.SearchResponse> mockResponses =
        Map.of(
            urn1, response1,
            urn2, response2);
    when(mockEsSearchDAO.rawEntity(opContext, urns)).thenReturn(mockResponses);

    // Execute the method
    Map<Urn, Map<String, Object>> result = testInstance.raw(opContext, urns);

    // Verify the results
    assertEquals(result.size(), 2);
    assertEquals(result.get(urn1), sourceMap1);
    assertEquals(result.get(urn2), sourceMap2);

    // Verify ESSearchDAO.rawEntity was called with the correct parameters
    verify(mockEsSearchDAO).rawEntity(opContext, urns);
  }

  @Test
  public void testRaw_WithEmptyHits() {
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    testInstance =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockEsSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    // Create test data
    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset1,PROD)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset2,PROD)");
    Set<Urn> urns = Set.of(urn1, urn2);

    // Create search response with empty hits for the first URN
    org.opensearch.search.SearchHits emptySearchHits = mock(org.opensearch.search.SearchHits.class);
    when(emptySearchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[] {});

    org.opensearch.action.search.SearchResponse emptyResponse =
        mock(org.opensearch.action.search.SearchResponse.class);
    when(emptyResponse.getHits()).thenReturn(emptySearchHits);

    // Create normal response for the second URN
    org.opensearch.search.SearchHit hit = mock(org.opensearch.search.SearchHit.class);
    Map<String, Object> sourceMap = Map.of("field1", "value", "field2", 456);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    org.opensearch.search.SearchHits searchHits = mock(org.opensearch.search.SearchHits.class);
    when(searchHits.getHits()).thenReturn(new org.opensearch.search.SearchHit[] {hit});

    org.opensearch.action.search.SearchResponse response =
        mock(org.opensearch.action.search.SearchResponse.class);
    when(response.getHits()).thenReturn(searchHits);

    // Mock the rawEntity response from ESSearchDAO
    Map<Urn, org.opensearch.action.search.SearchResponse> mockResponses =
        Map.of(
            urn1, emptyResponse,
            urn2, response);
    when(mockEsSearchDAO.rawEntity(opContext, urns)).thenReturn(mockResponses);

    // Execute the method
    Map<Urn, Map<String, Object>> result = testInstance.raw(opContext, urns);

    // Verify the results - should only have one entry for urn2
    assertEquals(result.size(), 1);
    assertEquals(result.get(urn2), sourceMap);
    assertFalse(result.containsKey(urn1));

    // Verify ESSearchDAO.rawEntity was called with the correct parameters
    verify(mockEsSearchDAO).rawEntity(opContext, urns);
  }

  @Test
  public void testRaw_WithNullHits() {
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    testInstance =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockEsSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);
    // Create test data
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset1,PROD)");
    Set<Urn> urns = Set.of(urn);

    // Create search response with null hits
    org.opensearch.search.SearchHits nullSearchHits = mock(org.opensearch.search.SearchHits.class);
    when(nullSearchHits.getHits()).thenReturn(null);

    org.opensearch.action.search.SearchResponse nullHitsResponse =
        mock(org.opensearch.action.search.SearchResponse.class);
    when(nullHitsResponse.getHits()).thenReturn(nullSearchHits);

    // Mock the rawEntity response from ESSearchDAO
    Map<Urn, org.opensearch.action.search.SearchResponse> mockResponses =
        Map.of(urn, nullHitsResponse);
    when(mockEsSearchDAO.rawEntity(opContext, urns)).thenReturn(mockResponses);

    // Execute the method
    Map<Urn, Map<String, Object>> result = testInstance.raw(opContext, urns);

    // Verify the results - should be empty since hits are null
    assertTrue(result.isEmpty());

    // Verify ESSearchDAO.rawEntity was called with the correct parameters
    verify(mockEsSearchDAO).rawEntity(opContext, urns);
  }

  @Test
  public void testRaw_WithEmptyUrns() {
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    testInstance =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockEsSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);
    // Create empty set of URNs
    Set<Urn> emptyUrns = Collections.emptySet();

    // Mock the rawEntity response from ESSearchDAO
    Map<Urn, org.opensearch.action.search.SearchResponse> emptyResponses = Collections.emptyMap();
    when(mockEsSearchDAO.rawEntity(opContext, emptyUrns)).thenReturn(emptyResponses);

    // Execute the method
    Map<Urn, Map<String, Object>> result = testInstance.raw(opContext, emptyUrns);

    // Verify the results are empty
    assertTrue(result.isEmpty());

    // Verify ESSearchDAO.rawEntity was called with the correct parameters
    verify(mockEsSearchDAO).rawEntity(opContext, emptyUrns);
  }

  @Test
  public void testBrowseV2WithNullCount() {
    // Test single entity browseV2 with null count - should use default limit
    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(opContext, ENTITY_NAME, PATH, null, INPUT, START, null);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(),
        DEFAULT_LIMIT,
        "Null count should result in default limit being used");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2WithValidCount() {
    // Test single entity browseV2 with valid count
    int validCount = 50;
    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(
            opContext, ENTITY_NAME, PATH, null, INPUT, START, validCount);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(), validCount, "Valid count should be passed through");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2WithMaxCount() {
    // Test single entity browseV2 with max count
    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(
            opContext, ENTITY_NAME, PATH, null, INPUT, START, MAX_LIMIT);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(limitCaptor.getValue().intValue(), MAX_LIMIT, "Max count should be allowed");
    assertNotNull(result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBrowseV2WithExceedingCountStrictMode() {
    // Test single entity browseV2 with count exceeding max in strict mode
    esSearchServiceLimited.browseV2(
        opContext, ENTITY_NAME, PATH, null, INPUT, START, MAX_LIMIT + 1);
  }

  @Test
  public void testBrowseV2WithNegativeCount() {
    // Test single entity browseV2 with negative count - should use default limit
    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(opContext, ENTITY_NAME, PATH, null, INPUT, START, -10);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(),
        DEFAULT_LIMIT,
        "Negative count should result in default limit being used");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2MultipleEntitiesWithNullCount() {
    // Test multiple entities browseV2 with null count
    List<String> entityNames = Arrays.asList("dataset", "dataflow");

    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(opContext, entityNames, PATH, null, INPUT, START, null);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(entityNames),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(),
        DEFAULT_LIMIT,
        "Null count should result in default limit being used");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2MultipleEntitiesWithValidCount() {
    // Test multiple entities browseV2 with valid count
    List<String> entityNames = Arrays.asList("dataset", "dataflow");
    int validCount = 75;

    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(
            opContext, entityNames, PATH, null, INPUT, START, validCount);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(entityNames),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(), validCount, "Valid count should be passed through");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2WithNonStrictMode() {
    // Setup non-strict configuration
    ResultsLimitConfig nonStrictResultsConfig =
        ResultsLimitConfig.builder().apiDefault(DEFAULT_LIMIT).max(MAX_LIMIT).strict(false).build();

    LimitConfig nonStrictLimitConfig =
        LimitConfig.builder().results(nonStrictResultsConfig).build();

    SearchServiceConfiguration nonStrictSearchConfig =
        SearchServiceConfiguration.builder().limit(nonStrictLimitConfig).build();

    ElasticSearchService nonStrictService =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            nonStrictSearchConfig,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            esBrowseDAO,
            mock(ESWriteDAO.class));

    // Test with count exceeding max in non-strict mode
    BrowseResultV2 result =
        nonStrictService.browseV2(
            opContext, ENTITY_NAME, PATH, null, INPUT, START, MAX_LIMIT + 100);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(
        limitCaptor.getValue().intValue(),
        DEFAULT_LIMIT,
        "Non-strict mode should return default when limit exceeds max");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2WithZeroCount() {
    // Test browseV2 with zero count
    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(opContext, ENTITY_NAME, PATH, null, INPUT, START, 0);

    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(PATH),
            nullable(Filter.class),
            eq(INPUT),
            eq(START),
            limitCaptor.capture());

    assertEquals(limitCaptor.getValue().intValue(), 0, "Zero count should be allowed");
    assertNotNull(result);
  }

  @Test
  public void testBrowseV2DifferentInputParameters() {
    // Test with different valid input parameters
    String differentPath = "/test/different/path";
    String differentInput = "different-input";
    int differentStart = 10;

    BrowseResultV2 result =
        esSearchServiceLimited.browseV2(
            opContext, ENTITY_NAME, differentPath, null, differentInput, differentStart, 25);

    verify(esBrowseDAO)
        .browseV2(
            any(OperationContext.class),
            eq(ENTITY_NAME),
            eq(differentPath),
            nullable(Filter.class),
            eq(differentInput),
            eq(differentStart),
            eq(25));

    assertNotNull(result);
  }

  @Test
  public void testSearchWithNullSize() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    List<String> entityNames = Arrays.asList("dataset", "dataflow");
    String input = "test query";
    Filter filter = null;
    List<SortCriterion> sortCriteria = Collections.emptyList();
    int from = 0;
    List<String> facets = Arrays.asList("platform", "origin");

    SearchResult expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(100)
            .setFrom(from)
            .setPageSize(
                TEST_SEARCH_SERVICE_CONFIG
                    .getLimit()
                    .getResults()
                    .getApiDefault()); // Should use service config default

    when(mockSearchDAO.search(
            any(OperationContext.class),
            eq(entityNames),
            eq(input),
            eq(filter),
            eq(sortCriteria),
            eq(from),
            eq(null), // null size
            eq(facets)))
        .thenReturn(expectedResult);

    // Execute
    SearchResult result =
        serviceWithMockDAO.search(
            opContext,
            entityNames,
            input,
            filter,
            sortCriteria,
            from,
            null, // null size
            facets);

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .search(
            any(OperationContext.class),
            eq(entityNames),
            eq(input),
            eq(filter),
            eq(sortCriteria),
            eq(from),
            eq(null),
            eq(facets));
  }

  @Test
  public void testFilterWithNullSize() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    String entityName = "dataset";
    Filter filter = mock(Filter.class);
    List<SortCriterion> sortCriteria = Collections.emptyList();
    int from = 0;

    SearchResult expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(50)
            .setFrom(from)
            .setPageSize(100);

    when(mockSearchDAO.filter(
            any(OperationContext.class),
            eq(entityName),
            eq(filter),
            eq(sortCriteria),
            eq(from),
            eq(null) // null size
            ))
        .thenReturn(expectedResult);

    // Execute
    SearchResult result =
        serviceWithMockDAO.filter(
            opContext, entityName, filter, sortCriteria, from, null // null size
            );

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .filter(
            any(OperationContext.class),
            eq(entityName),
            eq(filter),
            eq(sortCriteria),
            eq(from),
            eq(null));
  }

  @Test
  public void testFullTextScrollWithNullSize() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    List<String> entities = Arrays.asList("dataset");
    String input = "search term";
    Filter postFilters = null;
    List<SortCriterion> sortCriteria = Collections.emptyList();
    String scrollId = "test-scroll-id";
    String keepAlive = "5m";
    List<String> facets = Collections.emptyList();

    ScrollResult expectedResult =
        new ScrollResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(100)
            .setPageSize(100)
            .setScrollId("next-scroll-id");

    when(mockSearchDAO.scroll(
            any(OperationContext.class),
            eq(entities),
            eq(input),
            eq(postFilters),
            eq(sortCriteria),
            eq(scrollId),
            eq(keepAlive),
            eq(null) // null size
            ))
        .thenReturn(expectedResult);

    // Execute
    ScrollResult result =
        serviceWithMockDAO.fullTextScroll(
            opContext,
            entities,
            input,
            postFilters,
            sortCriteria,
            scrollId,
            keepAlive,
            null, // null size
            facets);

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .scroll(
            any(OperationContext.class),
            eq(entities),
            eq(input),
            eq(postFilters),
            eq(sortCriteria),
            eq(scrollId),
            eq(keepAlive),
            eq(null));
  }

  @Test
  public void testStructuredScrollWithNullSize() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    List<String> entities = Arrays.asList("dataset", "dataflow");
    String input = "*";
    Filter postFilters = mock(Filter.class);
    List<SortCriterion> sortCriteria = Collections.emptyList();
    String scrollId = null; // Initial scroll
    String keepAlive = "1m";
    List<String> facets = Arrays.asList("platform");

    ScrollResult expectedResult =
        new ScrollResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(200)
            .setPageSize(100)
            .setScrollId("first-scroll-id");

    when(mockSearchDAO.scroll(
            any(OperationContext.class),
            eq(entities),
            eq(input),
            eq(postFilters),
            eq(sortCriteria),
            eq(scrollId),
            eq(keepAlive),
            eq(null) // null size
            ))
        .thenReturn(expectedResult);

    // Execute
    ScrollResult result =
        serviceWithMockDAO.structuredScroll(
            opContext,
            entities,
            input,
            postFilters,
            sortCriteria,
            scrollId,
            keepAlive,
            null, // null size
            facets);

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .scroll(
            any(OperationContext.class),
            eq(entities),
            eq(input),
            eq(postFilters),
            eq(sortCriteria),
            eq(scrollId),
            eq(keepAlive),
            eq(null));
  }

  @Test
  public void testAggregateByValueWithNullLimit() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    List<String> entityNames = Arrays.asList("dataset");
    String field = "platform";
    Filter requestParams = null;

    Map<String, Long> expectedResult = new HashMap<>();
    expectedResult.put("mysql", 25L);
    expectedResult.put("postgres", 15L);

    when(mockSearchDAO.aggregateByValue(
            any(OperationContext.class),
            eq(entityNames),
            eq(field),
            eq(requestParams),
            eq(null) // null limit
            ))
        .thenReturn(expectedResult);

    // Execute
    Map<String, Long> result =
        serviceWithMockDAO.aggregateByValue(
            opContext, entityNames, field, requestParams, null // null limit
            );

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .aggregateByValue(
            any(OperationContext.class), eq(entityNames), eq(field), eq(requestParams), eq(null));
  }

  @Test
  public void testAutoCompleteWithNullLimit() {
    // Setup
    ESSearchDAO mockSearchDAO = mock(ESSearchDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mockSearchDAO,
            mock(ESBrowseDAO.class),
            mockEsWriteDAO);

    String entityName = "dataset";
    String query = "test";
    String field = "name";
    Filter requestParams = null;

    AutoCompleteResult expectedResult = new AutoCompleteResult();
    expectedResult.setQuery(query);
    expectedResult.setSuggestions(new StringArray(Arrays.asList("test1", "test2", "testing")));

    when(mockSearchDAO.autoComplete(
            any(OperationContext.class),
            eq(entityName),
            eq(query),
            eq(field),
            eq(requestParams),
            eq(null) // null limit
            ))
        .thenReturn(expectedResult);

    // Execute
    AutoCompleteResult result =
        serviceWithMockDAO.autoComplete(
            opContext, entityName, query, field, requestParams, null // null limit
            );

    // Verify
    assertEquals(result, expectedResult);
    verify(mockSearchDAO)
        .autoComplete(
            any(OperationContext.class),
            eq(entityName),
            eq(query),
            eq(field),
            eq(requestParams),
            eq(null));
  }

  @Test
  public void testBrowseWithNullSize() {
    // Setup
    ESBrowseDAO mockBrowseDAO = mock(ESBrowseDAO.class);
    ElasticSearchService serviceWithMockDAO =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mock(MappingsBuilder.class),
            mock(SettingsBuilder.class),
            mock(ESSearchDAO.class),
            mockBrowseDAO,
            mockEsWriteDAO);

    String entityName = "dataset";
    String path = "/prod/kafka";
    Filter filters = null;
    int from = 0;

    BrowseResult expectedResult = new BrowseResult();
    expectedResult.setFrom(from);
    expectedResult.setPageSize(100);
    expectedResult.setNumEntities(50);

    when(mockBrowseDAO.browse(
            any(OperationContext.class),
            eq(entityName),
            eq(path),
            eq(filters),
            eq(from),
            eq(null) // null size
            ))
        .thenReturn(expectedResult);

    // Execute
    BrowseResult result =
        serviceWithMockDAO.browse(
            opContext, entityName, path, filters, from, null // null size
            );

    // Verify
    assertEquals(result, expectedResult);
    verify(mockBrowseDAO)
        .browse(
            any(OperationContext.class), eq(entityName), eq(path), eq(filters), eq(from), eq(null));
  }
}
