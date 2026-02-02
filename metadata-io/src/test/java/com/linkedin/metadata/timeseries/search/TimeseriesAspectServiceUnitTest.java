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
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.TimeseriesScrollResult;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.pegasus2avro.entity.EnvelopedAspect;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
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

  private final SearchClientShim searchClient = mock(SearchClientShim.class);
  private final IndexConvention indexConvention = mock(IndexConvention.class);
  private final ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
  private final RawResponse response = mock(RawResponse.class);
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(
          SearchContext.EMPTY.toBuilder().indexConvention(indexConvention).build());
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
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
          indexBuilder,
          null);

  private static final String INDEX_PATTERN = "indexPattern";

  @BeforeMethod
  public void resetMocks() {
    reset(searchClient, indexConvention, bulkProcessor, response, indexBuilder);
  }

  @Test
  public void testGetIndicesIntegerWrap() throws IOException {
    when(indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn(INDEX_PATTERN);
    when(searchClient.performLowLevelRequest(any(Request.class))).thenReturn(response);
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

    HttpEntity responseEntity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(responseEntity);
    when(responseEntity.getContent())
        .thenReturn(IOUtils.toInputStream(jsonNode.toString(), StandardCharsets.UTF_8));

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
    when(searchClient.performLowLevelRequest(any(Request.class))).thenReturn(response);

    HttpEntity responseEntity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(responseEntity);
    when(responseEntity.getContent())
        .thenReturn(IOUtils.toInputStream("invalid json", StandardCharsets.UTF_8));

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
  public void testScrollAspectsReturnsScrollIdWhenFullPage() throws IOException {
    // Test that scrollId is returned when we get a full page of results (indicating more data)
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    int requestedCount = 2;

    // Create two mock hits to match requested count (full page)
    SearchHit mockHit1 = mock(SearchHit.class);
    Map<String, Object> sourceMap1 = new HashMap<>();
    sourceMap1.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:123");
    sourceMap1.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap1.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    when(mockHit1.getSourceAsMap()).thenReturn(sourceMap1);
    when(mockHit1.getSortValues()).thenReturn(new Object[] {1234567890L, "msg1"});

    SearchHit mockHit2 = mock(SearchHit.class);
    Map<String, Object> sourceMap2 = new HashMap<>();
    sourceMap2.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:456");
    sourceMap2.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567880L);
    sourceMap2.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567880L);
    when(mockHit2.getSourceAsMap()).thenReturn(sourceMap2);
    when(mockHit2.getSortValues()).thenReturn(new Object[] {1234567880L, "msg2"});

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit1, mockHit2});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(100, TotalHits.Relation.EQUAL_TO));

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute with count = 2 (same as number of hits)
    TimeseriesScrollResult result =
        _timeseriesAspectService.scrollAspects(
            opContext,
            "dataset",
            "datasetProfile",
            null,
            Arrays.asList(
                new SortCriterion()
                    .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                    .setOrder(SortOrder.DESCENDING)),
            null,
            requestedCount,
            null,
            null);

    // Verify scrollId is returned when we get a full page
    Assert.assertEquals(result.getNumResults(), 100);
    Assert.assertEquals(result.getPageSize(), 2);
    Assert.assertNotNull(result.getScrollId(), "scrollId should be returned for full page");

    // Verify the scrollId can be decoded
    SearchAfterWrapper wrapper = SearchAfterWrapper.fromScrollId(result.getScrollId());
    Assert.assertNotNull(wrapper);
    Assert.assertNotNull(wrapper.getSort());
    Assert.assertEquals(wrapper.getSort().length, 2, "Expected 2 sort values");
    // Sort values should match the last hit's sort values
    // Note: Sort values may be serialized as different numeric types, so compare as strings
    Assert.assertEquals(String.valueOf(wrapper.getSort()[0]), "1234567880");
    Assert.assertEquals(String.valueOf(wrapper.getSort()[1]), "msg2");
  }

  @Test
  public void testScrollAspectsNoScrollIdWhenPartialPage() throws IOException {
    // Test that scrollId is NOT returned when we get fewer results than requested (last page)
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    int requestedCount = 10;

    // Create only one mock hit (less than requested count)
    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:123");
    sourceMap.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockHit.getSortValues()).thenReturn(new Object[] {1234567890L, "msg1"});

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    // Execute with count = 10 but only 1 result returned
    TimeseriesScrollResult result =
        _timeseriesAspectService.scrollAspects(
            opContext,
            "dataset",
            "datasetProfile",
            null,
            Arrays.asList(
                new SortCriterion()
                    .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                    .setOrder(SortOrder.DESCENDING)),
            null,
            requestedCount,
            null,
            null);

    // Verify scrollId is NOT returned when we get a partial page (last page)
    Assert.assertEquals(result.getNumResults(), 1);
    Assert.assertEquals(result.getPageSize(), 1);
    Assert.assertNull(result.getScrollId(), "scrollId should be null for partial/last page");
  }

  @Test
  public void testScrollAspectsNoScrollIdWhenEmptyResults() throws IOException {
    // Test that scrollId is NOT returned when we get no results
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("datasetProfile")))
        .thenReturn("dataset_datasetProfile_index_v1");

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {});
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));

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

    // Verify scrollId is NOT returned when no results
    Assert.assertEquals(result.getNumResults(), 0);
    Assert.assertEquals(result.getPageSize(), 0);
    Assert.assertNull(result.getScrollId(), "scrollId should be null for empty results");
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

  @Test
  public void testRawWithNullUrnAspects() {
    // Test with null input
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, null);
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithEmptyUrnAspects() {
    // Test with empty input
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, Collections.emptyMap());
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithNonTimeseriesAspects() {
    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put("urn:li:dataset:123", new HashSet<>(Arrays.asList("status")));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify - should be empty since no timeseries aspects
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithValidTimeseriesAspect() throws IOException {
    // Setup
    String urnString = "urn:li:dataset:123";
    String aspectName = "datasetProfile";
    String indexName = "dataset_datasetProfile_index_v1";

    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName)))
        .thenReturn(indexName);

    // Mock search response
    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(MappingsBuilder.URN_FIELD, urnString);
    sourceMap.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap.put("field1", "value1");
    sourceMap.put("field2", "value2");
    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString, new HashSet<>(Arrays.asList(aspectName)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify
    Assert.assertEquals(result.size(), 1);
    Urn expectedUrn = UrnUtils.getUrn(urnString);
    Assert.assertTrue(result.containsKey(expectedUrn));
    Assert.assertTrue(result.get(expectedUrn).containsKey(aspectName));
    Assert.assertEquals(result.get(expectedUrn).get(aspectName), sourceMap);
  }

  @Test
  public void testRawWithMultipleAspects() throws IOException {
    // Setup
    String urnString = "urn:li:dataset:123";
    String aspectName1 = "datasetProfile";
    String aspectName2 = "operation";

    // Mock aspect specs
    AspectSpec mockSpec1 = mock(AspectSpec.class);
    when(mockSpec1.isTimeseries()).thenReturn(true);
    AspectSpec mockSpec2 = mock(AspectSpec.class);
    when(mockSpec2.isTimeseries()).thenReturn(true);

    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName1)))
        .thenReturn("dataset_datasetProfile_index_v1");
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName2)))
        .thenReturn("dataset_operation_index_v1");

    // Mock search responses for both aspects
    Map<String, Object> sourceMap1 = new HashMap<>();
    sourceMap1.put(MappingsBuilder.URN_FIELD, urnString);
    sourceMap1.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap1.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567890L);
    sourceMap1.put("profileField", "profileValue");

    Map<String, Object> sourceMap2 = new HashMap<>();
    sourceMap2.put(MappingsBuilder.URN_FIELD, urnString);
    sourceMap2.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567891L);
    sourceMap2.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1234567891L);
    sourceMap2.put("operationField", "operationValue");

    SearchHit mockHit1 = mock(SearchHit.class);
    when(mockHit1.getSourceAsMap()).thenReturn(sourceMap1);
    SearchHits mockHits1 = mock(SearchHits.class);
    when(mockHits1.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
    when(mockHits1.getHits()).thenReturn(new SearchHit[] {mockHit1});
    SearchResponse mockResponse1 = mock(SearchResponse.class);
    when(mockResponse1.getHits()).thenReturn(mockHits1);

    SearchHit mockHit2 = mock(SearchHit.class);
    when(mockHit2.getSourceAsMap()).thenReturn(sourceMap2);
    SearchHits mockHits2 = mock(SearchHits.class);
    when(mockHits2.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
    when(mockHits2.getHits()).thenReturn(new SearchHit[] {mockHit2});
    SearchResponse mockResponse2 = mock(SearchResponse.class);
    when(mockResponse2.getHits()).thenReturn(mockHits2);

    when(searchClient.search(any(), any())).thenReturn(mockResponse1).thenReturn(mockResponse2);

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString, new HashSet<>(Arrays.asList(aspectName1, aspectName2)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify
    Assert.assertEquals(result.size(), 1);
    Urn expectedUrn = UrnUtils.getUrn(urnString);
    Assert.assertTrue(result.containsKey(expectedUrn));
    Assert.assertEquals(result.get(expectedUrn).size(), 2);
    Assert.assertTrue(result.get(expectedUrn).containsKey(aspectName1));
    Assert.assertTrue(result.get(expectedUrn).containsKey(aspectName2));
    Assert.assertEquals(result.get(expectedUrn).get(aspectName1), sourceMap1);
    Assert.assertEquals(result.get(expectedUrn).get(aspectName2), sourceMap2);
  }

  @Test
  public void testRawWithNoResults() throws IOException {
    // Setup
    String urnString = "urn:li:dataset:123";
    String aspectName = "datasetProfile";

    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName)))
        .thenReturn("dataset_datasetProfile_index_v1");

    // Mock empty search response
    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));
    when(mockHits.getHits()).thenReturn(new SearchHit[] {});

    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(searchClient.search(any(), any())).thenReturn(mockResponse);

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString, new HashSet<>(Arrays.asList(aspectName)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify - should be empty since no documents found
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithSearchException() throws IOException {
    // Setup
    String urnString = "urn:li:dataset:123";
    String aspectName = "datasetProfile";

    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName)))
        .thenReturn("dataset_datasetProfile_index_v1");

    // Mock search to throw IOException
    when(searchClient.search(any(), any())).thenThrow(new IOException("Search failed"));

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString, new HashSet<>(Arrays.asList(aspectName)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify - should return empty map and not throw exception
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithInvalidUrn() {
    // Setup
    String invalidUrnString = "invalid:urn:format";
    String aspectName = "datasetProfile";

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(invalidUrnString, new HashSet<>(Arrays.asList(aspectName)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify - should return empty map due to URN parsing error
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithNullAspectSet() throws IOException {
    // Setup
    String urnString = "urn:li:dataset:123";

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString, null); // null aspect set

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify - should handle null gracefully and return empty map
    Assert.assertEquals(result, Collections.emptyMap());
  }

  @Test
  public void testRawWithMultipleUrns() throws IOException {
    // Setup
    String urnString1 = "urn:li:dataset:123";
    String urnString2 = "urn:li:dataset:456";
    String aspectName = "datasetProfile";

    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq(aspectName)))
        .thenReturn("dataset_datasetProfile_index_v1");

    // Mock search responses for both URNs
    Map<String, Object> sourceMap1 = new HashMap<>();
    sourceMap1.put(MappingsBuilder.URN_FIELD, urnString1);
    sourceMap1.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567890L);
    sourceMap1.put("data", "value1");

    Map<String, Object> sourceMap2 = new HashMap<>();
    sourceMap2.put(MappingsBuilder.URN_FIELD, urnString2);
    sourceMap2.put(MappingsBuilder.TIMESTAMP_FIELD, 1234567891L);
    sourceMap2.put("data", "value2");

    SearchHit mockHit1 = mock(SearchHit.class);
    when(mockHit1.getSourceAsMap()).thenReturn(sourceMap1);
    SearchHits mockHits1 = mock(SearchHits.class);
    when(mockHits1.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
    when(mockHits1.getHits()).thenReturn(new SearchHit[] {mockHit1});
    SearchResponse mockResponse1 = mock(SearchResponse.class);
    when(mockResponse1.getHits()).thenReturn(mockHits1);

    SearchHit mockHit2 = mock(SearchHit.class);
    when(mockHit2.getSourceAsMap()).thenReturn(sourceMap2);
    SearchHits mockHits2 = mock(SearchHits.class);
    when(mockHits2.getTotalHits()).thenReturn(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
    when(mockHits2.getHits()).thenReturn(new SearchHit[] {mockHit2});
    SearchResponse mockResponse2 = mock(SearchResponse.class);
    when(mockResponse2.getHits()).thenReturn(mockHits2);

    when(searchClient.search(any(), any())).thenReturn(mockResponse1).thenReturn(mockResponse2);

    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put(urnString1, new HashSet<>(Arrays.asList(aspectName)));
    urnAspects.put(urnString2, new HashSet<>(Arrays.asList(aspectName)));

    // Execute
    Map<Urn, Map<String, Map<String, Object>>> result =
        _timeseriesAspectService.raw(opContext, urnAspects);

    // Verify
    Assert.assertEquals(result.size(), 2);
    Urn expectedUrn1 = UrnUtils.getUrn(urnString1);
    Urn expectedUrn2 = UrnUtils.getUrn(urnString2);
    Assert.assertTrue(result.containsKey(expectedUrn1));
    Assert.assertTrue(result.containsKey(expectedUrn2));
    Assert.assertEquals(result.get(expectedUrn1).get(aspectName), sourceMap1);
    Assert.assertEquals(result.get(expectedUrn2).get(aspectName), sourceMap2);
  }
}
