package com.linkedin.metadata.dataHubUsage;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchResponse;
import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.LogInEvent;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import com.linkedin.metadata.datahubusage.event.UpdatePolicyEvent;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class DataHubUsageServiceTest {

  @Mock private RestHighLevelClient mockElasticClient;

  @Mock private IndexConvention mockIndexConvention;

  private OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

  @Mock private SearchResponse mockSearchResponse;

  @Mock private SearchHits mockSearchHits;

  private DataHubUsageServiceImpl dataHubUsageService;
  private final String TEST_INDEX_NAME = "datahub_usage_event_v1";

  private AutoCloseable mocks;

  @BeforeTest
  public void init() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @BeforeMethod
  public void setup() throws Exception {
    // Setup mock index convention to return test index name
    when(mockIndexConvention.getIndexName(DATAHUB_USAGE_EVENT_INDEX)).thenReturn(TEST_INDEX_NAME);

    // Initialize the service with mocks
    dataHubUsageService = new DataHubUsageServiceImpl(mockElasticClient, mockIndexConvention);

    // Mock search response setup
    when(mockElasticClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
  }

  @AfterMethod
  public void afterMethod() {
    Mockito.reset(mockElasticClient, mockIndexConvention, mockSearchHits, mockSearchResponse);
  }

  @AfterTest
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void testGetUsageIndexName() {
    String indexName = dataHubUsageService.getUsageIndexName();
    assertEquals(TEST_INDEX_NAME, indexName);
  }

  @Test
  public void testExternalAuditSearchWithEmptyResults() throws IOException {
    // Setup mock to return empty search results
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockSearchHits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));

    // Create a request with some filters
    ExternalAuditEventsSearchRequest.ExternalAuditEventsSearchRequestBuilder requestBuilder =
        ExternalAuditEventsSearchRequest.builder();
    requestBuilder.size(10);
    requestBuilder.eventTypes(
        Arrays.asList(
            DataHubUsageEventType.LOG_IN_EVENT.getType(),
            DataHubUsageEventType.CREATE_ACCESS_TOKEN_EVENT.getType()));
    requestBuilder.aspectTypes(Arrays.asList("entityProfile", "ownership"));
    requestBuilder.entityTypes(Arrays.asList("dataset", "dashboard"));
    requestBuilder.startTime(Instant.now().minus(7, ChronoUnit.DAYS).toEpochMilli());
    requestBuilder.endTime(Instant.now().toEpochMilli());

    ExternalAuditEventsSearchRequest request = requestBuilder.build();

    // Execute the search
    ExternalAuditEventsSearchResponse response =
        dataHubUsageService.externalAuditEventsSearch(opContext, request);

    // Verify the response for empty results
    assertNotNull(response);
    assertEquals(0, response.getCount());
    assertEquals(0, response.getTotal());
    assertEquals(0, response.getUsageEvents().size());
    assertNull(response.getNextScrollId());

    // Verify search request creation with correct filters
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(mockElasticClient).search(requestCaptor.capture(), any(RequestOptions.class));

    SearchRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_INDEX_NAME, capturedRequest.indices()[0]);
  }

  @Test
  public void testExternalAuditSearchWithResults() {
    // Create test search hits with mock data
    SearchHit hit1 =
        createMockSearchHit(
            DataHubUsageEventType.LOG_IN_EVENT.getType(), "urn:li:corpuser:user1", 1617235200000L);
    SearchHit hit2 =
        createMockSearchHit(
            DataHubUsageEventType.UPDATE_POLICY_EVENT.getType(),
            "urn:li:corpuser:user2",
            1617321600000L);

    // Setup mock to return the hits
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {hit1, hit2});
    when(mockSearchHits.getTotalHits()).thenReturn(new TotalHits(2, TotalHits.Relation.EQUAL_TO));

    // Create a request with minimal filters
    ExternalAuditEventsSearchRequest.ExternalAuditEventsSearchRequestBuilder requestBuilder =
        ExternalAuditEventsSearchRequest.builder();
    requestBuilder.size(10);
    requestBuilder.startTime(Instant.now().minus(7, ChronoUnit.DAYS).toEpochMilli());
    requestBuilder.endTime(Instant.now().toEpochMilli());
    ExternalAuditEventsSearchRequest request = requestBuilder.build();

    // Execute the search
    ExternalAuditEventsSearchResponse response =
        dataHubUsageService.externalAuditEventsSearch(opContext, request);

    // Verify the response data
    assertNotNull(response);
    assertEquals(2, response.getCount());
    assertEquals(2, response.getTotal());
    assertEquals(2, response.getUsageEvents().size());

    // Verify first event
    UsageEventResult event1 = response.getUsageEvents().get(0);
    assertEquals(LogInEvent.class.getSimpleName(), event1.getClass().getSimpleName());
    assertEquals(DataHubUsageEventType.LOG_IN_EVENT.getType(), event1.getEventType());
    LogInEvent logInEvent = (LogInEvent) event1;
    assertEquals(LoginSource.PASSWORD_LOGIN, logInEvent.getLoginSource());
    assertEquals("urn:li:corpuser:user1", logInEvent.getActorUrn());
    assertEquals(1617235200000L, logInEvent.getTimestamp());
    assertEquals(EventSource.GRAPHQL, logInEvent.getEventSource());

    // Verify second event
    UsageEventResult event2 = response.getUsageEvents().get(1);
    assertEquals(UpdatePolicyEvent.class.getSimpleName(), event2.getClass().getSimpleName());
    assertEquals(DataHubUsageEventType.UPDATE_POLICY_EVENT.getType(), event2.getEventType());
    UpdatePolicyEvent updatePolicyEvent = (UpdatePolicyEvent) event2;
    assertEquals("urn:li:corpuser:datahub", updatePolicyEvent.getEntityUrn());
    assertEquals("urn:li:corpuser:user2", updatePolicyEvent.getActorUrn());
    assertEquals(1617321600000L, updatePolicyEvent.getTimestamp());
  }

  @Test
  public void testExternalAuditSearchWithScrolling() throws IOException {
    // Create test search hit with sort values for scroll simulation
    SearchHit hit =
        createMockSearchHit(
            DataHubUsageEventType.CREATE_ACCESS_TOKEN_EVENT.getType(),
            "urn:li:corpuser:user1",
            1617235200000L);
    when(hit.getSortValues()).thenReturn(new Object[] {"sortval1", "sortval2"});

    // Setup mock to return the hit
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {hit});
    when(mockSearchHits.getTotalHits())
        .thenReturn(new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));

    // Create a request with a small size to test scrolling
    ExternalAuditEventsSearchRequest.ExternalAuditEventsSearchRequestBuilder requestBuilder =
        ExternalAuditEventsSearchRequest.builder();
    requestBuilder.size(1); // Only return 1 result to trigger scrolling
    requestBuilder.startTime(Instant.now().minus(7, ChronoUnit.DAYS).toEpochMilli());
    requestBuilder.endTime(Instant.now().toEpochMilli());

    ExternalAuditEventsSearchRequest request = requestBuilder.build();

    ExternalAuditEventsSearchResponse response =
        dataHubUsageService.externalAuditEventsSearch(opContext, request);

    // Verify the response includes a next scroll ID
    assertNotNull(response);
    assertEquals(1, response.getCount());
    assertNotNull(response.getNextScrollId());

    // Test using the scroll ID in a subsequent request
    ExternalAuditEventsSearchRequest.ExternalAuditEventsSearchRequestBuilder scrollRequest =
        ExternalAuditEventsSearchRequest.builder();
    scrollRequest.size(1);
    scrollRequest.scrollId(response.getNextScrollId());
    scrollRequest.startTime(request.getStartTime());
    scrollRequest.endTime(request.getEndTime());

    ExternalAuditEventsSearchResponse scrollResponse =
        dataHubUsageService.externalAuditEventsSearch(opContext, scrollRequest.build());

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(mockElasticClient, Mockito.times(2))
        .search(requestCaptor.capture(), any(RequestOptions.class));

    List<SearchRequest> capturedRequests = requestCaptor.getAllValues();
    assertEquals(2, capturedRequests.size());
  }

  @Test
  public void testExternalAuditSearchWithDefaultTimeRange() throws IOException {
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockSearchHits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));

    // Create a request with negative timestamps to test defaults
    ExternalAuditEventsSearchRequest.ExternalAuditEventsSearchRequestBuilder request =
        ExternalAuditEventsSearchRequest.builder();
    request.size(10);
    request.startTime(-1); // Should default to 1 day ago
    request.endTime(-1); // Should default to now

    dataHubUsageService.externalAuditEventsSearch(opContext, request.build());

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    Mockito.verify(mockElasticClient).search(requestCaptor.capture(), any(RequestOptions.class));
  }

  private SearchHit createMockSearchHit(String eventType, String actorUrn, long timestamp) {
    SearchHit mockHit = Mockito.mock(SearchHit.class);

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put(DataHubUsageEventConstants.TYPE, eventType);
    sourceMap.put(DataHubUsageEventConstants.ACTOR_URN, actorUrn);
    sourceMap.put(DataHubUsageEventConstants.TIMESTAMP, timestamp);
    sourceMap.put(DataHubUsageEventConstants.LOGIN_SOURCE, LoginSource.PASSWORD_LOGIN.getSource());
    sourceMap.put(
        DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    sourceMap.put(DataHubUsageEventConstants.SOURCE_IP, "123.123.123");
    sourceMap.put(DataHubUsageEventConstants.EVENT_SOURCE, EventSource.GRAPHQL.getSource());
    sourceMap.put(DataHubUsageEventConstants.USER_AGENT, "browser");
    sourceMap.put(DataHubUsageEventConstants.TRACE_ID, "traceid");
    sourceMap.put(DataHubUsageEventConstants.ENTITY_TYPE, "myEntity");
    sourceMap.put(DataHubUsageEventConstants.ENTITY_URN, "urn:li:corpuser:datahub");
    sourceMap.put(DataHubUsageEventConstants.ASPECT_NAME, "myAspect");

    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);

    return mockHit;
  }
}
