package com.linkedin.common.client;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.AspectsDoIngestProposalRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.client.response.BatchKVResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseClientTest {
  static final Authentication AUTH =
      new Authentication(new Actor(ActorType.USER, "fake"), "foo:bar");
  static final OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(AUTH);

  private Client mockRestliClient;
  private RestliEntityClient testClient;
  private ResponseFuture<String> mockFuture;
  private Response<String> mockResponse;
  private MetricUtils mockMetricUtils;

  @BeforeMethod
  public void setup() throws RemoteInvocationException {
    mockRestliClient = mock(Client.class);
    mockFuture = mock(ResponseFuture.class);
    mockResponse = mock(Response.class);
    when(mockFuture.getResponse()).thenReturn(mockResponse);

    // Setup MetricUtils mock
    mockMetricUtils = mock(MetricUtils.class);
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.empty());
  }

  @Test
  public void testZeroRetry() throws RemoteInvocationException {
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);
    ResponseFuture<String> mockFuture = mock(ResponseFuture.class);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    RestliEntityClient testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);
    testClient.sendClientRequest(testRequestBuilder, opContext);
    // Expected 1 actual try and 0 retries
    verify(mockRestliClient).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testMultipleRetries() throws RemoteInvocationException {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);
    ResponseFuture<String> mockFuture = mock(ResponseFuture.class);

    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenThrow(new RuntimeException())
        .thenReturn(mockFuture);

    RestliEntityClient testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(1)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);
    testClient.sendClientRequest(testRequestBuilder, opContext);
    // Expected 1 actual try and 1 retries
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testNonRetry() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);

    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenThrow(new RuntimeException(new RequiredFieldNotPresentException("value")));

    RestliEntityClient testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(1)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);
    assertThrows(
        RuntimeException.class, () -> testClient.sendClientRequest(testRequestBuilder, opContext));
  }

  @Test
  public void testBatchIngestProposalsSingleBatch()
      throws RemoteInvocationException, InterruptedException {
    // Create test MCPs
    List<MetadataChangeProposal> mcps = createTestMCPs(5);

    // Mock successful response
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("success");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(10) // Batch size larger than number of MCPs
                .batchIngestConcurrency(2)
                .build(),
            mockMetricUtils);

    List<String> result = testClient.batchIngestProposals(opContext, mcps, false);

    // Verify only one batch request was made
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));

    // Verify all URNs were returned
    assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      assertTrue(result.contains("urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)"));
    }
  }

  @Test
  public void testBatchIngestProposalsMultipleBatches()
      throws RemoteInvocationException, InterruptedException {
    // Create test MCPs - 15 total
    List<MetadataChangeProposal> mcps = createTestMCPs(15);

    // Mock successful responses for all batches
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("success");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(5) // Batch size of 5, so we expect 3 batches
                .batchIngestConcurrency(3)
                .build(),
            mockMetricUtils);

    List<String> result = testClient.batchIngestProposals(opContext, mcps, true);

    // Verify 3 batch requests were made (15 MCPs / 5 per batch)
    verify(mockRestliClient, times(3)).sendRequest(any(ActionRequest.class));

    // Verify all URNs were returned
    assertEquals(result.size(), 15);
  }

  @Test
  public void testBatchIngestProposalsWithFailure()
      throws RemoteInvocationException, InterruptedException {
    // Create test MCPs
    List<MetadataChangeProposal> mcps = createTestMCPs(10);

    // Mock failure response
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("failure");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(5)
                .batchIngestConcurrency(2)
                .build(),
            mockMetricUtils);

    List<String> result = testClient.batchIngestProposals(opContext, mcps, false);

    // Verify 2 batch requests were made
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));

    // Verify empty result due to failure
    assertEquals(result.size(), 0);
  }

  @Test
  public void testBatchIngestProposalsWithException() throws RemoteInvocationException {
    // Create test MCPs
    List<MetadataChangeProposal> mcps = createTestMCPs(5);

    // Mock exception - wrap RemoteInvocationException in RuntimeException
    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenThrow(new RuntimeException(new RemoteInvocationException("Test exception")));

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(10)
                .batchIngestConcurrency(2)
                .build(),
            mockMetricUtils);

    // Should throw RuntimeException wrapping the RemoteInvocationException
    assertThrows(
        RuntimeException.class, () -> testClient.batchIngestProposals(opContext, mcps, false));
  }

  @Test
  public void testBatchIngestProposalsAsyncParameter()
      throws RemoteInvocationException, InterruptedException {
    // Create test MCPs
    List<MetadataChangeProposal> mcps = createTestMCPs(5);

    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    when(mockRestliClient.sendRequest(requestCaptor.capture())).thenReturn(mockFuture);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("success");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(10)
                .batchIngestConcurrency(2)
                .build(),
            mockMetricUtils);

    // Test with async=true
    testClient.batchIngestProposals(opContext, mcps, true);

    ActionRequest capturedRequest = requestCaptor.getValue();
    assertTrue(capturedRequest.getInputRecord().toString().contains("async=true"));

    // Test with async=false
    testClient.batchIngestProposals(opContext, mcps, false);

    capturedRequest = requestCaptor.getValue();
    assertTrue(capturedRequest.getInputRecord().toString().contains("async=false"));
  }

  @Test
  public void testBatchIngestProposalsEmptyList() throws RemoteInvocationException {
    List<MetadataChangeProposal> mcps = Collections.emptyList();

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(10)
                .batchIngestConcurrency(2)
                .build(),
            mockMetricUtils);

    List<String> result = testClient.batchIngestProposals(opContext, mcps, false);

    // Should handle empty list gracefully
    assertEquals(result.size(), 0);
  }

  @Test
  public void testBatchIngestProposalsLargeBatch()
      throws RemoteInvocationException, InterruptedException {
    // Create a large number of MCPs to test batching behavior
    List<MetadataChangeProposal> mcps = createTestMCPs(100);

    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("success");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(25) // 100 MCPs / 25 per batch = 4 batches
                .batchIngestConcurrency(4)
                .build(),
            mockMetricUtils);

    List<String> result = testClient.batchIngestProposals(opContext, mcps, false);

    // Verify 4 batch requests were made
    verify(mockRestliClient, times(4)).sendRequest(any(ActionRequest.class));

    // Verify all URNs were returned
    assertEquals(result.size(), 100);
  }

  @Test
  public void testBatchIngestProposalsConcurrentExecution()
      throws RemoteInvocationException, InterruptedException {
    // Create test MCPs
    List<MetadataChangeProposal> mcps = createTestMCPs(20);

    // Add delay to simulate concurrent processing
    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(100); // Simulate some processing time
              return mockFuture;
            });
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockResponse.getEntity()).thenReturn("success");

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .batchIngestSize(5) // 20 MCPs / 5 per batch = 4 batches
                .batchIngestConcurrency(4) // Process all 4 batches concurrently
                .build(),
            mockMetricUtils);

    long startTime = System.currentTimeMillis();
    List<String> result = testClient.batchIngestProposals(opContext, mcps, false);
    long endTime = System.currentTimeMillis();

    // Verify all batches were processed
    verify(mockRestliClient, times(4)).sendRequest(any(ActionRequest.class));
    assertEquals(result.size(), 20);

    // With concurrent execution, should take ~100ms (not 400ms if sequential)
    // Adding some buffer for test execution overhead
    assertTrue(
        (endTime - startTime) < 300,
        "Concurrent execution took too long: " + (endTime - startTime) + "ms");
  }

  @Test
  public void testGet() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    // Create a real Entity object
    Entity entity = new Entity(new DataMap());

    // Setup mock to return Entity directly
    setupMockClientResponse(entity);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Entity result = testClient.get(opContext, testUrn);

    // Verify
    assertNotNull(result);
    assertEquals(result.data(), entity.data());
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testGetV2() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    // Create test aspects
    Map<String, RecordTemplate> aspects = new HashMap<>();
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setDescription("Test dataset");
    aspects.put("datasetProperties", datasetProperties);

    EntityResponse entityResponse = buildEntityResponse(aspects);

    // Setup mock to return EntityResponse
    setupMockClientResponse(entityResponse);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    EntityResponse result =
        testClient.getV2(
            opContext, "dataset", testUrn, Collections.singleton("datasetProperties"), true);

    // Verify
    assertNotNull(result);
    assertEquals(result.getAspects().size(), 1);
    assertTrue(result.getAspects().containsKey("datasetProperties"));
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testBatchGet() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Set<Urn> urns =
        IntStream.range(0, 30)
            .mapToObj(
                i -> {
                  try {
                    return Urn.createFromString(
                        "urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)");
                  } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    // Mock BatchKVResponse with the RestLi EntityResponse
    BatchKVResponse<String, com.linkedin.restli.common.EntityResponse> batchResponse =
        mock(BatchKVResponse.class);
    Map<String, com.linkedin.restli.common.EntityResponse> responseMap = new HashMap<>();

    for (Urn urn : urns) {
      // Create Entity
      Entity entity = new Entity(new DataMap());

      // Create RestLi EntityResponse
      com.linkedin.restli.common.EntityResponse entityResponse =
          new com.linkedin.restli.common.EntityResponse(Entity.class);
      entityResponse.setEntity(entity);
      entityResponse.setStatus(com.linkedin.restli.common.HttpStatus.S_200_OK);

      responseMap.put(urn.toString(), entityResponse);
    }

    when(batchResponse.getResults()).thenReturn(responseMap);

    // Setup mock response
    setupMockClientResponse(batchResponse);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Map<Urn, Entity> result = testClient.batchGet(opContext, urns);

    // Verify - should make 2 batch requests (30 items / 25 batch size)
    verify(mockRestliClient, times(2)).sendRequest(any(Request.class));
    assertEquals(result.size(), 30);
  }

  @Test
  public void testAutoCompleteWithField() throws RemoteInvocationException {
    // Setup
    AutoCompleteResult mockAutoCompleteResult = new AutoCompleteResult();
    mockAutoCompleteResult.setQuery("test");
    mockAutoCompleteResult.setSuggestions(new StringArray("test1", "test2", "test3"));

    Response<AutoCompleteResult> mockAutoCompleteResponse = mock(Response.class);
    ResponseFuture<AutoCompleteResult> mockAutoCompleteFuture = mock(ResponseFuture.class);

    when(mockAutoCompleteResponse.getEntity()).thenReturn(mockAutoCompleteResult);
    when(mockAutoCompleteFuture.getResponse()).thenReturn(mockAutoCompleteResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockAutoCompleteFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    AutoCompleteResult result =
        testClient.autoComplete(opContext, "dataset", "test", null, 10, "name");

    // Verify
    assertEquals(result.getQuery(), "test");
    assertEquals(result.getSuggestions().size(), 3);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testAutoComplete() throws RemoteInvocationException {
    // Setup
    AutoCompleteResult mockAutoCompleteResult = new AutoCompleteResult();
    mockAutoCompleteResult.setQuery("search");
    mockAutoCompleteResult.setSuggestions(new StringArray("searchResult1", "searchResult2"));

    setupMockClientResponse(mockAutoCompleteResult);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    AutoCompleteResult result = testClient.autoComplete(opContext, "dataset", "search", null, 10);

    // Verify
    assertEquals(result.getQuery(), "search");
    assertEquals(result.getSuggestions().size(), 2);
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testBrowse() throws RemoteInvocationException {
    // Setup
    BrowseResult mockBrowseResult = new BrowseResult();
    mockBrowseResult.setFrom(0);
    mockBrowseResult.setPageSize(10);
    mockBrowseResult.setNumEntities(2);

    Response<BrowseResult> mockBrowseResponse = mock(Response.class);
    ResponseFuture<BrowseResult> mockBrowseFuture = mock(ResponseFuture.class);

    when(mockBrowseResponse.getEntity()).thenReturn(mockBrowseResult);
    when(mockBrowseFuture.getResponse()).thenReturn(mockBrowseResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockBrowseFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Map<String, String> filters = new HashMap<>();
    filters.put("platform", "hdfs");
    BrowseResult result = testClient.browse(opContext, "dataset", "/prod", filters, 0, 10);

    // Verify
    assertEquals(result.getFrom(), 0);
    assertEquals(result.getPageSize(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testUpdate() throws RemoteInvocationException {
    // Setup
    Entity entity = new Entity(new DataMap());

    // For void methods, we can return null or an empty response
    setupMockClientResponse(null);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    testClient.update(opContext, entity);

    // Verify
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testUpdateWithSystemMetadata() throws RemoteInvocationException {
    // Setup - Create real Entity object instead of mock
    Entity entity = new Entity(new DataMap());

    SystemMetadata mockSystemMetadata = new SystemMetadata();
    mockSystemMetadata.setRunId("test-run-id");
    mockSystemMetadata.setLastObserved(System.currentTimeMillis());

    when(mockRestliClient.sendRequest(any(Request.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute with system metadata
    testClient.updateWithSystemMetadata(opContext, entity, mockSystemMetadata);

    // Verify
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("systemMetadata"));

    // Test with null system metadata (should delegate to update())
    testClient.updateWithSystemMetadata(opContext, entity, null);
    verify(mockRestliClient, times(2)).sendRequest(any(Request.class));
  }

  @Test
  public void testBatchUpdate() throws RemoteInvocationException {
    // Setup - Create real Entity objects instead of mocks
    Set<Entity> entities =
        IntStream.range(0, 5)
            .mapToObj(
                i -> {
                  Entity entity = new Entity(new DataMap());
                  return entity;
                })
            .collect(Collectors.toSet());

    when(mockRestliClient.sendRequest(any(Request.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    testClient.batchUpdate(opContext, entities);

    // Verify
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testSearch() throws RemoteInvocationException {
    // Setup
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(100);

    setupMockClientResponse(mockSearchResult);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    SearchResult result =
        testClient.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            "dataset",
            "test query",
            new HashMap<>(),
            0,
            10);

    // Verify
    assertEquals(result.getFrom(), 0);
    assertEquals(result.getPageSize(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testList() throws RemoteInvocationException {
    // Setup
    ListResult mockListResult = new ListResult();
    mockListResult.setStart(0);
    mockListResult.setCount(10);
    mockListResult.setTotal(50);

    Response<ListResult> mockListResponse = mock(Response.class);
    ResponseFuture<ListResult> mockListFuture = mock(ResponseFuture.class);

    when(mockListResponse.getEntity()).thenReturn(mockListResult);
    when(mockListFuture.getResponse()).thenReturn(mockListResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockListFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Map<String, String> filters = new HashMap<>();
    filters.put("platform", "hdfs");
    ListResult result = testClient.list(opContext, "dataset", filters, 0, 10);

    // Verify
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 50);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testSearchWithSortCriteria() throws RemoteInvocationException {
    // Setup
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);

    Response<SearchResult> mockSearchResponse = mock(Response.class);
    ResponseFuture<SearchResult> mockSearchFuture = mock(ResponseFuture.class);

    when(mockSearchResponse.getEntity()).thenReturn(mockSearchResult);
    when(mockSearchFuture.getResponse()).thenReturn(mockSearchResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockSearchFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Filter filter = new Filter();
    List<SortCriterion> sortCriteria =
        Collections.singletonList(
            new SortCriterion().setField("lastModified").setOrder(SortOrder.DESCENDING));

    SearchResult result =
        testClient.search(opContext, "dataset", "test", filter, sortCriteria, 0, 10);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("sort"));
  }

  @Test
  public void testSearchAcrossEntities() throws RemoteInvocationException {
    // Setup
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(25);

    Response<SearchResult> mockSearchResponse = mock(Response.class);
    ResponseFuture<SearchResult> mockSearchFuture = mock(ResponseFuture.class);

    when(mockSearchResponse.getEntity()).thenReturn(mockSearchResult);
    when(mockSearchFuture.getResponse()).thenReturn(mockSearchResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockSearchFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    List<String> entities = Arrays.asList("dataset", "dataFlow", "dataJob");
    Filter filter = new Filter();
    List<SortCriterion> sortCriteria = Collections.emptyList();

    SearchResult result =
        testClient.searchAcrossEntities(
            opContext, entities, "test query", filter, 0, 10, sortCriteria, null);

    // Verify
    assertEquals(result.getFrom(), 0);
    assertEquals(result.getPageSize(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testScrollAcrossEntities() throws RemoteInvocationException {
    // Setup
    ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setScrollId("scroll123");
    mockScrollResult.setNumEntities(10);

    Response<ScrollResult> mockScrollResponse = mock(Response.class);
    ResponseFuture<ScrollResult> mockScrollFuture = mock(ResponseFuture.class);

    when(mockScrollResponse.getEntity()).thenReturn(mockScrollResult);
    when(mockScrollFuture.getResponse()).thenReturn(mockScrollResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockScrollFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    List<String> entities = Arrays.asList("dataset", "dataFlow");
    ScrollResult result =
        testClient.scrollAcrossEntities(
            opContext,
            entities,
            "test",
            null,
            "scroll123",
            "5m",
            Collections.emptyList(),
            10,
            null);

    // Verify
    assertEquals(result.getScrollId(), "scroll123");
    assertEquals(result.getNumEntities(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testSearchAcrossLineage() throws RemoteInvocationException, URISyntaxException {
    // Setup
    LineageSearchResult mockLineageResult = new LineageSearchResult();
    mockLineageResult.setFrom(0);
    mockLineageResult.setPageSize(10);
    mockLineageResult.setNumEntities(5);

    Response<LineageSearchResult> mockLineageResponse = mock(Response.class);
    ResponseFuture<LineageSearchResult> mockLineageFuture = mock(ResponseFuture.class);

    when(mockLineageResponse.getEntity()).thenReturn(mockLineageResult);
    when(mockLineageFuture.getResponse()).thenReturn(mockLineageResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockLineageFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Urn sourceUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    List<String> entities = Arrays.asList("dataset");

    LineageSearchResult result =
        testClient.searchAcrossLineage(
            opContext,
            sourceUrn,
            LineageDirection.DOWNSTREAM,
            entities,
            "test",
            3,
            null,
            Collections.emptyList(),
            0,
            10);

    // Verify
    assertEquals(result.getFrom(), 0);
    assertEquals(result.getPageSize(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testSetWritable() throws RemoteInvocationException {
    // Setup
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute - set writable to true
    testClient.setWritable(opContext, true);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("value=true"));

    // Execute - set writable to false
    testClient.setWritable(opContext, false);
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testBatchGetTotalEntityCount() throws RemoteInvocationException {
    // Setup
    Map<String, Long> mockCounts = new HashMap<>();
    mockCounts.put("dataset", 100L);
    mockCounts.put("dataFlow", 50L);
    mockCounts.put("dataJob", 25L);

    Response<Map<String, Long>> mockCountResponse = mock(Response.class);
    ResponseFuture<Map<String, Long>> mockCountFuture = mock(ResponseFuture.class);

    when(mockCountResponse.getEntity()).thenReturn(mockCounts);
    when(mockCountFuture.getResponse()).thenReturn(mockCountResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockCountFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    List<String> entities = Arrays.asList("dataset", "dataFlow", "dataJob");
    Map<String, Long> result = testClient.batchGetTotalEntityCount(opContext, entities, null);

    // Verify
    assertEquals(result.get("dataset"), Long.valueOf(100L));
    assertEquals(result.get("dataFlow"), Long.valueOf(50L));
    assertEquals(result.get("dataJob"), Long.valueOf(25L));
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testListUrns() throws RemoteInvocationException {
    // Setup
    ListUrnsResult mockListUrnsResult = new ListUrnsResult();
    mockListUrnsResult.setStart(0);
    mockListUrnsResult.setCount(10);
    mockListUrnsResult.setTotal(100);

    Response<ListUrnsResult> mockListUrnsResponse = mock(Response.class);
    ResponseFuture<ListUrnsResult> mockListUrnsFuture = mock(ResponseFuture.class);

    when(mockListUrnsResponse.getEntity()).thenReturn(mockListUrnsResult);
    when(mockListUrnsFuture.getResponse()).thenReturn(mockListUrnsResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockListUrnsFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    ListUrnsResult result = testClient.listUrns(opContext, "dataset", 0, 10);

    // Verify
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 100);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testDeleteEntity() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    testClient.deleteEntity(opContext, testUrn);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains(testUrn.toString()));
  }

  @Test
  public void testDeleteEntityReferences() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    testClient.deleteEntityReferences(opContext, testUrn);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains(testUrn.toString()));
  }

  @Test
  public void testFilter() throws RemoteInvocationException {
    // Setup
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(15);

    Response<SearchResult> mockSearchResponse = mock(Response.class);
    ResponseFuture<SearchResult> mockSearchFuture = mock(ResponseFuture.class);

    when(mockSearchResponse.getEntity()).thenReturn(mockSearchResult);
    when(mockSearchFuture.getResponse()).thenReturn(mockSearchResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockSearchFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray());
    List<SortCriterion> sortCriteria = Collections.emptyList();

    SearchResult result = testClient.filter(opContext, "dataset", filter, sortCriteria, 0, 10);

    // Verify
    assertEquals(result.getFrom(), 0);
    assertEquals(result.getPageSize(), 10);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testExists() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    Response<Boolean> mockBoolResponse = mock(Response.class);
    ResponseFuture<Boolean> mockBoolFuture = mock(ResponseFuture.class);

    when(mockBoolResponse.getEntity()).thenReturn(true);
    when(mockBoolFuture.getResponse()).thenReturn(mockBoolResponse);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockBoolFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute - with default includeSoftDeleted
    boolean result = testClient.exists(opContext, testUrn);

    // Verify
    assertTrue(result);
    verify(mockRestliClient, times(1)).sendRequest(any(ActionRequest.class));

    // Execute - with explicit includeSoftDeleted
    when(mockBoolResponse.getEntity()).thenReturn(false);
    boolean result2 = testClient.exists(opContext, testUrn, false);
    assertFalse(result2);
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testGetAspectOrNull() throws RemoteInvocationException {
    // Setup for successful case
    VersionedAspect mockVersionedAspect = new VersionedAspect();
    mockVersionedAspect.setVersion(1L);

    setupMockClientResponse(mockVersionedAspect);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute - successful case
    VersionedAspect result =
        testClient.getAspectOrNull(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "datasetProperties",
            1L);

    // Verify
    assertNotNull(result);
    assertEquals(result.getVersion(), Long.valueOf(1L));

    // Setup for 404 case - need to reconfigure the mock
    RestLiResponseException notFoundException = mock(RestLiResponseException.class);
    when(notFoundException.getStatus()).thenReturn(404);

    // Create new mock future that throws the exception
    ResponseFuture<VersionedAspect> mockFutureFor404 = mock(ResponseFuture.class);
    when(mockFutureFor404.getResponse()).thenThrow(notFoundException);

    // Reset the client mock to return the new future for the next call
    when(mockRestliClient.sendRequest(any(Request.class))).thenReturn(mockFutureFor404);

    // Execute - 404 case
    VersionedAspect nullResult =
        testClient.getAspectOrNull(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "nonExistentAspect",
            1L);

    // Verify
    assertNull(nullResult);
  }

  @Test
  public void testGetTimeseriesAspectValues() throws RemoteInvocationException {
    // Setup
    EnvelopedAspectArray aspectArray = new EnvelopedAspectArray();
    aspectArray.add(new EnvelopedAspect());
    aspectArray.add(new EnvelopedAspect());

    GetTimeseriesAspectValuesResponse response = new GetTimeseriesAspectValuesResponse();
    response.setValues(aspectArray);

    // Setup mock
    setupMockClientResponse(response);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    List<EnvelopedAspect> result =
        testClient.getTimeseriesAspectValues(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "dataset",
            "datasetProfile",
            System.currentTimeMillis() - 86400000L, // 1 day ago
            System.currentTimeMillis(),
            10,
            null,
            null);

    // Verify
    assertEquals(result.size(), 2);
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testGetVersionedAspect() throws RemoteInvocationException {
    // Setup
    VersionedAspect mockVersionedAspect = new VersionedAspect();

    // Create the aspect data with the correct structure
    DataMap aspectDataMap = new DataMap();
    DataMap datasetPropertiesData = new DataMap();
    datasetPropertiesData.put("description", "Test dataset");

    // Use DatasetProperties canonical name
    aspectDataMap.put("com.linkedin.dataset.DatasetProperties", datasetPropertiesData);

    mockVersionedAspect.setAspect(new Aspect(aspectDataMap));
    mockVersionedAspect.setVersion(1L);

    setupMockClientResponse(mockVersionedAspect);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute - use DatasetProperties.class instead of RecordTemplate.class
    Optional<DatasetProperties> result =
        testClient.getVersionedAspect(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "datasetProperties",
            1L,
            DatasetProperties.class);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(result.get().getDescription(), "Test dataset");
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));

    // Test 404 case
    RestLiResponseException notFoundException = mock(RestLiResponseException.class);
    when(notFoundException.getStatus()).thenReturn(404);

    ResponseFuture<VersionedAspect> mock404Future = mock(ResponseFuture.class);
    when(mock404Future.getResponse()).thenThrow(notFoundException);

    when(mockRestliClient.sendRequest(any(Request.class))).thenReturn(mock404Future);

    Optional<DatasetProperties> emptyResult =
        testClient.getVersionedAspect(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "nonExistent",
            1L,
            DatasetProperties.class);

    assertFalse(emptyResult.isPresent());
  }

  @Test
  public void testProducePlatformEvent() throws Exception {
    // Setup
    PlatformEvent mockEvent = new PlatformEvent();
    mockEvent.setName("TestEvent");

    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute with key
    testClient.producePlatformEvent(opContext, "TestEvent", "testKey", mockEvent);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("TestEvent"));

    // Execute without key
    testClient.producePlatformEvent(opContext, "TestEvent", null, mockEvent);
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testRollbackIngestion() throws Exception {
    // Setup
    Authorizer mockAuthorizer = mock(Authorizer.class);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    testClient.rollbackIngestion(opContext, "test-run-id", mockAuthorizer);

    // Verify
    ArgumentCaptor<ActionRequest> requestCaptor = ArgumentCaptor.forClass(ActionRequest.class);
    verify(mockRestliClient, times(1)).sendRequest(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("test-run-id"));
    assertTrue(requestCaptor.getValue().getInputRecord().toString().contains("dryRun=false"));
  }

  // Helper method to create test MCPs
  private List<MetadataChangeProposal> createTestMCPs(int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i -> {
              MetadataChangeProposal mcp = new MetadataChangeProposal();
              mcp.setEntityType("dataset");
              try {
                mcp.setEntityUrn(
                    Urn.createFromString(
                        "urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)"));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              mcp.setAspectName("datasetProperties");
              mcp.setChangeType(ChangeType.UPSERT);
              return mcp;
            })
        .collect(Collectors.toList());
  }

  @Test
  public void testGetAspect() throws RemoteInvocationException {
    // Setup
    VersionedAspect mockVersionedAspect = new VersionedAspect();
    mockVersionedAspect.setVersion(1L);
    mockVersionedAspect.setAspect(new Aspect(new DataMap()));

    setupMockClientResponse(mockVersionedAspect);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    VersionedAspect result =
        testClient.getAspect(
            opContext,
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)",
            "datasetProperties",
            1L);

    // Verify
    assertNotNull(result);
    assertEquals(result.getVersion(), Long.valueOf(1L));
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testScrollAcrossLineage() throws RemoteInvocationException, URISyntaxException {
    // Setup
    LineageScrollResult mockLineageScrollResult = new LineageScrollResult();
    mockLineageScrollResult.setScrollId("lineage-scroll-123");
    mockLineageScrollResult.setNumEntities(5);

    setupMockClientResponse(mockLineageScrollResult);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Urn sourceUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    List<String> entities = Arrays.asList("dataset");

    LineageScrollResult result =
        testClient.scrollAcrossLineage(
            opContext,
            sourceUrn,
            LineageDirection.DOWNSTREAM,
            entities,
            "test",
            3,
            null,
            Collections.emptyList(),
            "scroll123",
            "5m",
            10);

    // Verify
    assertEquals(result.getScrollId(), "lineage-scroll-123");
    assertEquals(result.getNumEntities(), 5);
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testGetBrowsePaths() throws RemoteInvocationException, URISyntaxException {
    // Setup
    StringArray mockPaths = new StringArray();
    mockPaths.add("/prod/data");
    mockPaths.add("/test/data");

    setupMockClientResponse(mockPaths);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Urn testUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    StringArray result = testClient.getBrowsePaths(opContext, testUrn);

    // Verify
    assertEquals(result.size(), 2);
    assertTrue(result.contains("/prod/data"));
    assertTrue(result.contains("/test/data"));
    verify(mockRestliClient, times(1)).sendRequest(any(Request.class));
  }

  @Test
  public void testBatchGetV2() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Set<Urn> urns =
        IntStream.range(0, 15)
            .mapToObj(
                i -> {
                  try {
                    return Urn.createFromString(
                        "urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)");
                  } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    // Create mock batch response with RestLi EntityResponse
    BatchKVResponse<String, com.linkedin.restli.common.EntityResponse> batchResponse =
        mock(BatchKVResponse.class);
    Map<String, com.linkedin.restli.common.EntityResponse> responseMap = new HashMap<>();

    for (Urn urn : urns) {
      Map<String, RecordTemplate> aspects = new HashMap<>();
      DatasetProperties props = new DatasetProperties();
      props.setDescription("Test dataset for " + urn);
      aspects.put("datasetProperties", props);

      // Build the com.linkedin.entity.EntityResponse
      com.linkedin.entity.EntityResponse entityResponse = buildEntityResponse(aspects);

      // Wrap it in RestLi's EntityResponse
      com.linkedin.restli.common.EntityResponse restliEntityResponse =
          new com.linkedin.restli.common.EntityResponse(com.linkedin.entity.EntityResponse.class);
      restliEntityResponse.setEntity(entityResponse);
      restliEntityResponse.setStatus(com.linkedin.restli.common.HttpStatus.S_200_OK);

      responseMap.put(urn.toString(), restliEntityResponse);
    }

    when(batchResponse.getResults()).thenReturn(responseMap);

    // Setup mock
    setupMockClientResponse(batchResponse);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Map<Urn, com.linkedin.entity.EntityResponse> result =
        testClient.batchGetV2(
            opContext, "dataset", urns, Collections.singleton("datasetProperties"), true);

    // Verify - should make 2 batch requests (15 items / 10 batch size)
    verify(mockRestliClient, times(2)).sendRequest(any(Request.class));
    assertEquals(result.size(), 15);
  }

  @Test
  public void testBatchGetVersionedV2() throws RemoteInvocationException, URISyntaxException {
    // Setup
    Set<com.linkedin.common.VersionedUrn> versionedUrns =
        IntStream.range(0, 15)
            .mapToObj(
                i -> {
                  try {
                    Urn urn =
                        Urn.createFromString(
                            "urn:li:dataset:(urn:li:dataPlatform:hdfs,test" + i + ",PROD)");
                    return new com.linkedin.common.VersionedUrn().setUrn(urn).setVersionStamp("1");
                  } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    // Create mock response with RestLi EntityResponse
    BatchKVResponse<com.linkedin.common.urn.VersionedUrn, com.linkedin.restli.common.EntityResponse>
        batchResponse = mock(BatchKVResponse.class);
    Map<com.linkedin.common.urn.VersionedUrn, com.linkedin.restli.common.EntityResponse>
        responseMap = new HashMap<>();

    for (com.linkedin.common.VersionedUrn versionedUrn : versionedUrns) {
      // Create the entity response using RestLi's EntityResponse
      com.linkedin.entity.EntityResponse entityResponse = buildEntityResponse(new HashMap<>());

      // Create RestLi EntityResponse wrapper
      com.linkedin.restli.common.EntityResponse restliEntityResponse =
          new com.linkedin.restli.common.EntityResponse(EntityResponse.class);
      restliEntityResponse.setEntity(entityResponse);
      restliEntityResponse.setStatus(com.linkedin.restli.common.HttpStatus.S_200_OK);

      com.linkedin.common.urn.VersionedUrn key =
          com.linkedin.common.urn.VersionedUrn.of(
              versionedUrn.getUrn().toString(), versionedUrn.getVersionStamp());
      responseMap.put(key, restliEntityResponse);
    }

    when(batchResponse.getResults()).thenReturn(responseMap);

    // Setup mock
    setupMockClientResponse(batchResponse);

    testClient =
        new RestliEntityClient(
            mockRestliClient,
            EntityClientConfig.builder()
                .backoffPolicy(new ExponentialBackoff(1))
                .retryCount(0)
                .batchGetV2Size(10)
                .batchGetV2Concurrency(2)
                .build(),
            mockMetricUtils);

    // Execute
    Map<Urn, com.linkedin.entity.EntityResponse> result =
        testClient.batchGetVersionedV2(
            opContext, "dataset", versionedUrns, Collections.singleton("datasetProperties"));

    // Verify - should make 2 batch requests (15 items / 10 batch size)
    verify(mockRestliClient, times(2)).sendRequest(any(Request.class));
    assertEquals(result.size(), 15);
  }

  private <T> void setupMockClientResponse(T responseEntity) throws RemoteInvocationException {
    Response<T> mockResponse = mock(Response.class);
    ResponseFuture<T> mockFuture = mock(ResponseFuture.class);

    when(mockResponse.getEntity()).thenReturn(responseEntity);
    when(mockFuture.getResponse()).thenReturn(mockResponse);
    when(mockRestliClient.sendRequest(any(Request.class))).thenReturn(mockFuture);
  }

  private EntityResponse buildEntityResponse(Map<String, RecordTemplate> aspects) {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    for (Map.Entry<String, RecordTemplate> entry : aspects.entrySet()) {
      aspectMap.put(
          entry.getKey(),
          new com.linkedin.entity.EnvelopedAspect()
              .setValue(new com.linkedin.entity.Aspect(entry.getValue().data())));
    }
    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }
}
