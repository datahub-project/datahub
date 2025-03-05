package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));
    testInstance =
        new ElasticSearchService(
            indexBuilders, mock(ESSearchDAO.class), mock(ESBrowseDAO.class), mockEsWriteDAO);
  }

  @Test
  public void testAppendRunId_ValidRunId() {
    String runId = "test-run-id";

    // Execute
    testInstance.appendRunId(opContext, TEST_URN, runId);

    // Capture and verify the script update parameters
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> scriptCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptCaptor.capture(),
            upsertCaptor.capture());

    // Verify script content
    String expectedScript =
        String.format(
            "if (ctx._source.containsKey('runId')) { "
                + "if (!ctx._source.runId.contains('%s')) { "
                + "ctx._source.runId.add('%s'); "
                + "if (ctx._source.runId.length > %s) { ctx._source.runId.remove(0) } } "
                + "} else { ctx._source.runId = ['%s'] }",
            runId, runId, MAX_RUN_IDS_INDEXED, runId);
    assertEquals(scriptCaptor.getValue(), expectedScript);

    // Verify upsert document
    Map<String, Object> capturedUpsert = upsertCaptor.getValue();
    assertEquals(capturedUpsert.get("runId"), Collections.singletonList(runId));
  }

  @Test
  public void testAppendRunId_NullRunId() {
    // Execute with null runId
    testInstance.appendRunId(opContext, TEST_URN, null);

    // Verify the script update is still called with null handling
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> scriptCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptCaptor.capture(),
            upsertCaptor.capture());

    // Verify script content handles null
    String expectedScript =
        String.format(
            "if (ctx._source.containsKey('runId')) { "
                + "if (!ctx._source.runId.contains('%s')) { "
                + "ctx._source.runId.add('%s'); "
                + "if (ctx._source.runId.length > %s) { ctx._source.runId.remove(0) } } "
                + "} else { ctx._source.runId = ['%s'] }",
            null, null, MAX_RUN_IDS_INDEXED, null);
    assertEquals(scriptCaptor.getValue(), expectedScript);

    Map<String, Object> capturedUpsert = upsertCaptor.getValue();
    assertEquals(capturedUpsert.get("runId"), Collections.singletonList(null));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRunId_NullUrn() {
    testInstance.appendRunId(opContext, null, "test-run-id");
  }

  @Test
  public void testRaw_WithValidUrns() {
    // Mock dependencies
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));

    // Create test instance with mocked ESSearchDAO
    testInstance =
        new ElasticSearchService(
            indexBuilders, mockEsSearchDAO, mock(ESBrowseDAO.class), mockEsWriteDAO);

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
    // Mock dependencies
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));

    // Create test instance with mocked ESSearchDAO
    testInstance =
        new ElasticSearchService(
            indexBuilders, mockEsSearchDAO, mock(ESBrowseDAO.class), mockEsWriteDAO);

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
    // Mock dependencies
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));

    // Create test instance with mocked ESSearchDAO
    testInstance =
        new ElasticSearchService(
            indexBuilders, mockEsSearchDAO, mock(ESBrowseDAO.class), mockEsWriteDAO);

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
    // Mock dependencies
    ESSearchDAO mockEsSearchDAO = mock(ESSearchDAO.class);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));

    // Create test instance with mocked ESSearchDAO
    testInstance =
        new ElasticSearchService(
            indexBuilders, mockEsSearchDAO, mock(ESBrowseDAO.class), mockEsWriteDAO);

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
}
