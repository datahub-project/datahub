package com.linkedin.metadata.timeseries.search;

import static io.datahubproject.test.search.SearchTestUtils.TEST_TIMESERIES_ASPECT_SERVICE_CONFIG;
import static org.mockito.Mockito.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.TimeseriesScrollResult;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.pegasus2avro.entity.EnvelopedAspect;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test using mocks instead of integration for testing functionality not dependent on a real server
 * response
 */
public class TimeseriesAspectServiceUnitTest {

  private final RestHighLevelClient searchClient = mock(RestHighLevelClient.class);
  private final IndexConvention indexConvention = mock(IndexConvention.class);
  private final ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
  private final RestClient restClient = mock(RestClient.class);
  private final EntityRegistry entityRegistry = mock(EntityRegistry.class);
  private final ESIndexBuilder indexBuilder = mock(ESIndexBuilder.class);
  private final TimeseriesAspectService _timeseriesAspectService =
      new ElasticSearchTimeseriesAspectService(
          searchClient,
          bulkProcessor,
          0,
          QueryFilterRewriteChain.EMPTY,
          TEST_TIMESERIES_ASPECT_SERVICE_CONFIG,
          entityRegistry,
          indexConvention,
          indexBuilder);
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(indexConvention);

  private static final String INDEX_PATTERN = "indexPattern";

  @BeforeMethod
  public void resetMocks() {
    reset(searchClient, indexConvention, bulkProcessor, restClient, entityRegistry, indexBuilder);
  }

  @Test
  public void testGetIndicesIntegerWrap() throws IOException {
    when(indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn(INDEX_PATTERN);
    when(searchClient.getLowLevelClient()).thenReturn(restClient);
    ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
    ObjectNode indicesNode = JsonNodeFactory.instance.objectNode();
    ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
    ObjectNode primariesNode = JsonNodeFactory.instance.objectNode();
    ObjectNode storeNode = JsonNodeFactory.instance.objectNode();
    NumericNode bytesNode = JsonNodeFactory.instance.numberNode(8078398031L);
    storeNode.set("size_in_bytes", bytesNode);
    primariesNode.set("store", storeNode);
    indexNode.set("primaries", primariesNode);
    indicesNode.set("someIndexName", indexNode);
    jsonNode.set("indices", indicesNode);

    Response response = mock(Response.class);
    HttpEntity responseEntity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(responseEntity);
    when(responseEntity.getContent())
        .thenReturn(IOUtils.toInputStream(jsonNode.toString(), StandardCharsets.UTF_8));
    when(restClient.performRequest(any(Request.class))).thenReturn(response);

    List<TimeseriesIndexSizeResult> results = _timeseriesAspectService.getIndexSizes(opContext);

    Assert.assertEquals(results.get(0).getSizeInMb(), 8078.398031);
  }

  @Test
  public void testSearchQueryFailure() throws IOException {
    // setup mock
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testAspect")))
        .thenReturn("dataset_testAspect_index_v1");

    // Setup search request that will fail
    when(searchClient.search(any(), any())).thenThrow(new IOException("Search failed"));

    Filter filter = QueryUtils.newFilter("field", "value");

    // Verify that ESQueryException is thrown with correct message
    try {
      _timeseriesAspectService.getAspectValues(
          opContext,
          UrnUtils.getUrn("urn:li:dataset:123"),
          "dataset",
          "testAspect",
          null,
          null,
          10,
          filter,
          null);
      Assert.fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals(e.getCause().getMessage(), "Search failed");
    }
  }

  @Test
  public void testScrollSearchQueryFailure() throws IOException {
    // setup mock
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testAspect")))
        .thenReturn("dataset_testAspect_index_v1");

    // Setup search request that will fail
    when(searchClient.search(any(), any())).thenThrow(new IOException("Scroll search failed"));

    Filter filter = QueryUtils.newFilter("field", "value");
    List<SortCriterion> sortCriteria =
        Arrays.asList(new SortCriterion().setField("timestamp").setOrder(SortOrder.DESCENDING));

    // Verify that ESQueryException is thrown with correct message
    try {
      _timeseriesAspectService.scrollAspects(
          opContext, "dataset", "testAspect", filter, sortCriteria, null, 10, null, null);
      Assert.fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals(e.getCause().getMessage(), "Scroll search failed");
    }
  }

  @Test
  public void testParseDocumentJsonProcessingException() throws IOException {
    // Setup mock for parseDocument failure
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testProfile")))
        .thenReturn("dataset_testProfile_index_v1");

    // Create a mock ObjectMapper that throws JsonProcessingException
    ObjectMapper mockObjectMapper = mock(ObjectMapper.class);
    when(mockObjectMapper.writeValueAsString(any()))
        .thenThrow(new JsonProcessingException("Test JSON processing error") {});

    // Mock the operation context to return our mock ObjectMapper
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.getObjectMapper()).thenReturn(mockObjectMapper);
    when(mockOpContext.getSearchContext()).thenReturn(opContext.getSearchContext());
    when(mockOpContext.getEntityRegistry()).thenReturn(opContext.getEntityRegistry());

    // Mock withSpan to directly execute the supplier
    when(mockOpContext.withSpan(anyString(), any(Supplier.class), anyString(), anyString()))
        .thenAnswer(
            invocation -> {
              Supplier<?> supplier = invocation.getArgument(1);
              return supplier.get();
            });

    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.EVENT_FIELD, "some_event_data");
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute and verify RuntimeException is thrown
    try {
      _timeseriesAspectService.getAspectValues(
          mockOpContext,
          UrnUtils.getUrn("urn:li:dataset:123"),
          "dataset",
          "testProfile",
          null,
          null,
          10,
          null,
          null);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("Failed to deserialize event from the timeseries aspect index"));
    }
  }

  @Test
  public void testParseDocumentSystemMetadataJsonProcessingException() throws IOException {
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.EVENT_FIELD, "valid_event");
    sourceMap.put(
        "systemMetadata",
        new Object() {
          @Override
          public String toString() {
            throw new RuntimeException("Cannot serialize");
          }
        });
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute and verify RuntimeException is thrown
    try {
      _timeseriesAspectService.getAspectValues(
          opContext,
          UrnUtils.getUrn("urn:li:dataset:123"),
          "dataset",
          "datasetProfile",
          null,
          null,
          10,
          null,
          null);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains("Failed to deserialize system metadata from the timeseries aspect index"));
    }
  }

  @Test
  public void testGetIndexSizesJsonProcessingException() throws IOException {
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    // Setup mock to throw IOException when reading JSON response
    when(indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn(INDEX_PATTERN);
    when(searchClient.getLowLevelClient()).thenReturn(restClient);

    Response response = mock(Response.class);
    HttpEntity responseEntity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(responseEntity);
    when(responseEntity.getContent())
        .thenReturn(IOUtils.toInputStream("invalid json", StandardCharsets.UTF_8));
    when(restClient.performRequest(any(Request.class))).thenReturn(response);

    // Execute and verify RuntimeException is thrown
    try {
      _timeseriesAspectService.getIndexSizes(opContext);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
    }
  }

  @Test
  public void testScrollAspectsWithEventField() throws IOException {
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:123");
    sourceMap.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.EVENT_FIELD, "{\"test\":\"value\"}");
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute
    TimeseriesScrollResult result =
        _timeseriesAspectService.scrollAspects(
            opContext,
            "dataset",
            "datasetProfile",
            null,
            Collections.emptyList(),
            null,
            10,
            null,
            null);

    // Verify
    Assert.assertEquals(result.getNumResults(), 1);
    Assert.assertEquals(result.getPageSize(), 1);
    Assert.assertNotNull(result.getEvents());
    Assert.assertNotNull(result.getDocuments());
  }

  @Test
  public void testScrollAspectsWithEventFieldJsonException() throws IOException {
    // Setup mock for scroll search with invalid event field
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testProfile")))
        .thenReturn("dataset_testProfile_index_v1");

    // Create a mock ObjectMapper that throws JsonProcessingException
    ObjectMapper mockObjectMapper = mock(ObjectMapper.class);
    when(mockObjectMapper.writeValueAsString(any()))
        .thenThrow(new JsonProcessingException("Test JSON processing error") {});

    // Mock the operation context to return our mock ObjectMapper
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.getObjectMapper()).thenReturn(mockObjectMapper);
    when(mockOpContext.getSearchContext()).thenReturn(opContext.getSearchContext());
    when(mockOpContext.getEntityRegistry()).thenReturn(opContext.getEntityRegistry());
    when(mockOpContext.withSpan(anyString(), any(Supplier.class), anyString(), anyString()))
        .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(1)).get());

    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:123");
    sourceMap.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.EVENT_FIELD, "some_event");
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute and verify RuntimeException is thrown
    try {
      _timeseriesAspectService.scrollAspects(
          mockOpContext,
          "dataset",
          "testProfile",
          null,
          Collections.emptyList(),
          null,
          10,
          null,
          null);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("Failed to deserialize event from the timeseries aspect index"));
    }
  }

  @Test
  public void testScrollAspectsWithoutEventField() throws IOException {
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:123");
    sourceMap.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap.put("customField1", "value1");
    sourceMap.put("customField2", "value2");
    // No EVENT_FIELD
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute
    TimeseriesScrollResult result =
        _timeseriesAspectService.scrollAspects(
            opContext,
            "dataset",
            "datasetProfile",
            null,
            Collections.emptyList(),
            null,
            10,
            null,
            null);

    // Verify
    Assert.assertEquals(result.getNumResults(), 1);
    Assert.assertEquals(result.getPageSize(), 1);
    Assert.assertNull(result.getEvents().get(0)); // No enveloped aspect when no event field
    Assert.assertNotNull(result.getDocuments());

    // The event should be a Map containing the custom fields
    Object event = result.getDocuments().get(0).getEvent();
    Assert.assertTrue(event instanceof Map);
    Map<String, Object> eventMap = (Map<String, Object>) event;
    Assert.assertEquals(eventMap.get("customField1"), "value1");
    Assert.assertEquals(eventMap.get("customField2"), "value2");
  }

  @Test
  public void testGetLatestTimeseriesAspectValuesWithInterruptedException() throws Exception {
    // Setup
    when(indexConvention.getTimeseriesAspectIndexName(anyString(), anyString()))
        .thenReturn("test_index");

    // Mock the executor to throw InterruptedException
    ExecutorService mockExecutor = mock(ExecutorService.class);
    Future<Pair<String, EnvelopedAspect>> mockFuture = mock(Future.class);
    when(mockFuture.get()).thenThrow(new InterruptedException("Test interruption"));
    when(mockExecutor.submit(any(Callable.class))).thenReturn(mockFuture);

    // Use reflection to inject the mock executor
    Field queryPoolField = ElasticSearchTimeseriesAspectService.class.getDeclaredField("queryPool");
    queryPoolField.setAccessible(true);
    queryPoolField.set(_timeseriesAspectService, mockExecutor);

    Set<Urn> urns = new HashSet<>();
    urns.add(UrnUtils.getUrn("urn:li:dataset:123"));
    Set<String> aspectNames = new HashSet<>();
    aspectNames.add("testAspect");

    // Execute and verify
    try {
      _timeseriesAspectService.getLatestTimeseriesAspectValues(opContext, urns, aspectNames, null);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof InterruptedException);
    }
  }

  @Test
  public void testGetLatestTimeseriesAspectValuesWithExecutionException() throws Exception {
    // Setup
    when(indexConvention.getTimeseriesAspectIndexName(anyString(), anyString()))
        .thenReturn("test_index");

    // Mock the executor to throw ExecutionException
    ExecutorService mockExecutor = mock(ExecutorService.class);
    Future<Pair<String, EnvelopedAspect>> mockFuture = mock(Future.class);
    when(mockFuture.get())
        .thenThrow(new ExecutionException("Test execution error", new RuntimeException()));
    when(mockExecutor.submit(any(Callable.class))).thenReturn(mockFuture);

    // Use reflection to inject the mock executor
    Field queryPoolField = ElasticSearchTimeseriesAspectService.class.getDeclaredField("queryPool");
    queryPoolField.setAccessible(true);
    queryPoolField.set(_timeseriesAspectService, mockExecutor);

    Set<Urn> urns = new HashSet<>();
    urns.add(UrnUtils.getUrn("urn:li:dataset:123"));
    Set<String> aspectNames = new HashSet<>();
    aspectNames.add("testAspect");

    // Execute and verify
    try {
      _timeseriesAspectService.getLatestTimeseriesAspectValues(opContext, urns, aspectNames, null);
      Assert.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof ExecutionException);
    }
  }
}
