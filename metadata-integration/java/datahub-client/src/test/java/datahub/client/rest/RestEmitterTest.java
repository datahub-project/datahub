package datahub.client.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.dataset.DatasetProperties;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.server.TestDataHubServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.RequestDefinition;

import static org.mockserver.model.HttpRequest.*;


@RunWith(MockitoJUnitRunner.class)
public class RestEmitterTest {

  @Mock
  HttpAsyncClientBuilder mockHttpClientFactory;

  @Mock
  CloseableHttpAsyncClient mockClient;

  @Captor
  ArgumentCaptor<HttpPost> postArgumentCaptor;

  @Captor
  ArgumentCaptor<FutureCallback> callbackCaptor;

  @Before
  public void setupMocks() {
    Mockito.when(mockHttpClientFactory.build()).thenReturn(mockClient);
  }

  @Test
  public void testPost() throws URISyntaxException, IOException {

    RestEmitter emitter = RestEmitter.create(b -> b.asyncHttpClientBuilder(mockHttpClientFactory));
    MetadataChangeProposalWrapper mcp =
        getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)");
    emitter.emit(mcp, null);
    Mockito.verify(mockClient).execute(postArgumentCaptor.capture(), callbackCaptor.capture());
    FutureCallback callback = callbackCaptor.getValue();
    Assert.assertNotNull(callback);
    HttpPost testPost = postArgumentCaptor.getValue();
    Assert.assertEquals("2.0.0", testPost.getFirstHeader("X-RestLi-Protocol-Version").getValue());
    InputStream is = testPost.getEntity().getContent();
    byte[] contentBytes = new byte[(int) testPost.getEntity().getContentLength()];
    is.read(contentBytes);
    String contentString = new String(contentBytes, StandardCharsets.UTF_8);
    String expectedContent = "{\"proposal\":{\"aspectName\":\"datasetProperties\","
        + "\"entityUrn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)\","
        + "\"entityType\":\"dataset\",\"changeType\":\"UPSERT\",\"aspect\":{\"contentType\":\"application/json\""
        + ",\"value\":\"{\\\"description\\\":\\\"Test Dataset\\\"}\"}}}";
    Assert.assertEquals(expectedContent, contentString);
  }
  
  @Test
  public void testExceptions() throws URISyntaxException, IOException, ExecutionException, InterruptedException {

    RestEmitter emitter = RestEmitter.create($ -> $.asyncHttpClientBuilder(mockHttpClientFactory));

    MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.create(b -> b.entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("Test Dataset")));

    Future<HttpResponse> mockFuture = Mockito.mock(Future.class);
    Mockito.when(mockClient.execute(Mockito.any(), Mockito.any())).thenReturn(mockFuture);
    Mockito.when(mockFuture.get()).thenThrow(new ExecutionException("Test execution exception", null));
    try {
      emitter.emit(mcp, null).get();
      Assert.fail("should not be here");
    } catch (ExecutionException e) {
      Assert.assertEquals(e.getMessage(), "Test execution exception");
    }
  }

  @Test
  public void testExtraHeaders() throws Exception {
    RestEmitter emitter = RestEmitter.create(b -> b.asyncHttpClientBuilder(mockHttpClientFactory)
        .extraHeaders(Collections.singletonMap("Test-Header", "Test-Value")));
    MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.create(
        b -> b.entityType("dataset").entityUrn("urn:li:dataset:foo").upsert().aspect(new DatasetProperties()));
    Future<HttpResponse> mockFuture = Mockito.mock(Future.class);
    Mockito.when(mockClient.execute(Mockito.any(), Mockito.any())).thenReturn(mockFuture);
    emitter.emit(mcpw, null);
    Mockito.verify(mockClient).execute(postArgumentCaptor.capture(), callbackCaptor.capture());
    FutureCallback callback = callbackCaptor.getValue();
    Assert.assertNotNull(callback);
    HttpPost testPost = postArgumentCaptor.getValue();
    // old headers are not modified
    Assert.assertEquals("2.0.0", testPost.getFirstHeader("X-RestLi-Protocol-Version").getValue());
    // new headers are added
    Assert.assertEquals("Test-Value", testPost.getFirstHeader("Test-Header").getValue());
  }

  @Test
  public void mockServerTest() throws InterruptedException, ExecutionException, IOException {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));
    Assert.assertTrue(emitter.testConnection());
  }

  @Test
  public void multithreadedTestExecutors() throws Exception {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));

    testDataHubServer.getMockServer()
        .when(request().withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal")
            .withHeader("Content-type", "application/json"), Times.unlimited())
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(200));
    ExecutorService executor = Executors.newFixedThreadPool(10);
    ArrayList<Future> results = new ArrayList();
    Random random = new Random();
    int testIteration = random.nextInt();
    int numRequests = 100;
    for (int i = 0; i < numRequests; ++i) {
      int finalI = i;
      results.add(executor.submit(() -> {
        try {
          Thread.sleep(random.nextInt(100));
          MetadataChangeProposalWrapper mcp =
              getMetadataChangeProposalWrapper(String.format("Test Dataset %d", testIteration),
                  String.format("urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar-%d,PROD)", finalI));
          Future<MetadataWriteResponse> future = emitter.emit(mcp, null);
          MetadataWriteResponse response = future.get();
          Assert.assertTrue(response.isSuccess());
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }));
    }
    results.forEach(x -> {
      try {
        x.get();
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    });
    RequestDefinition[] recordedRequests =
        testDataHubServer.getMockServer().retrieveRecordedRequests(request().withPath("/aspects").withMethod("POST"));
    Assert.assertEquals(100, recordedRequests.length);
    List<HttpRequest> requests = Arrays.stream(recordedRequests)
        .sequential()
        .filter(x -> x instanceof HttpRequest)
        .map(x -> (HttpRequest) x)
        .collect(Collectors.toList());
    ObjectMapper mapper = new ObjectMapper();
    for (int i = 0; i < numRequests; ++i) {
      String expectedContent = String.format("{\"proposal\":{\"aspectName\":\"datasetProperties\","
          + "\"entityUrn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar-%d,PROD)\","
          + "\"entityType\":\"dataset\",\"changeType\":\"UPSERT\",\"aspect\":{\"contentType\":\"application/json\""
          + ",\"value\":\"{\\\"description\\\":\\\"Test Dataset %d\\\"}\"}}}", i, testIteration);

      Assert.assertEquals(requests.stream().filter(x -> {
        String bodyString = "";
        try {
          bodyString = mapper.writeValueAsString(
              mapper.readValue(x.getBodyAsString().getBytes(StandardCharsets.UTF_8), Map.class));
        } catch (IOException ioException) {
          return false;
        }
        return bodyString.equals(expectedContent);
      }).count(), 1);
    }
  }

  private MetadataChangeProposalWrapper getMetadataChangeProposalWrapper(String description, String entityUrn) {
    return MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(entityUrn)
        .upsert()
        .aspect(new DatasetProperties().setDescription(description))
        .build();
  }

  @Test
  public void multithreadedTestSingleThreadCaller() throws Exception {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));

    testDataHubServer.getMockServer()
        .when(request().withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal")
            .withHeader("Content-type", "application/json"), Times.unlimited())
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(200));
    ArrayList<Future> results = new ArrayList();
    Random random = new Random();
    int testIteration = random.nextInt();
    int numRequests = 100;
    for (int i = 0; i < numRequests; ++i) {
      MetadataChangeProposalWrapper mcp =
          getMetadataChangeProposalWrapper(String.format("Test Dataset %d", testIteration),
              String.format("urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar-%d,PROD)", i));
      Future<MetadataWriteResponse> future = emitter.emit(mcp, null);
      results.add(future);
    }
    results.forEach(x -> {
      try {
        x.get();
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    });
    RequestDefinition[] recordedRequests =
        testDataHubServer.getMockServer().retrieveRecordedRequests(request().withPath("/aspects").withMethod("POST"));
    Assert.assertEquals(numRequests, recordedRequests.length);
    List<HttpRequest> requests = Arrays.stream(recordedRequests)
        .sequential()
        .filter(x -> x instanceof HttpRequest)
        .map(x -> (HttpRequest) x)
        .collect(Collectors.toList());
    ObjectMapper mapper = new ObjectMapper();
    for (int i = 0; i < numRequests; ++i) {
      String expectedContent = String.format("{\"proposal\":{\"aspectName\":\"datasetProperties\","
          + "\"entityUrn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,foo.bar-%d,PROD)\","
          + "\"entityType\":\"dataset\",\"changeType\":\"UPSERT\",\"aspect\":{\"contentType\":\"application/json\""
          + ",\"value\":\"{\\\"description\\\":\\\"Test Dataset %d\\\"}\"}}}", i, testIteration);

      Assert.assertEquals(requests.stream().filter(x -> {
        String bodyString = "";
        try {
          bodyString = mapper.writeValueAsString(
              mapper.readValue(x.getBodyAsString().getBytes(StandardCharsets.UTF_8), Map.class));
        } catch (IOException ioException) {
          return false;
        }
        return bodyString.equals(expectedContent);
      }).count(), 1);
    }
  }

  @Test
  public void testCallback() throws Exception {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));

    testDataHubServer.getMockServer()
        .when(request().withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal")
            .withHeader("Content-type", "application/json"), Times.unlimited())
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(500).withBody("exception"));

    MetadataChangeProposalWrapper mcpw = getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:foo");
    AtomicReference<MetadataWriteResponse> callbackResponse = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    Future<MetadataWriteResponse> future = emitter.emit(mcpw, new Callback() {
      @Override
      public void onCompletion(MetadataWriteResponse response) {
        callbackResponse.set(response);
        Assert.assertFalse(response.isSuccess());
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable exception) {
        Assert.fail("Should not be called");
        latch.countDown();
      }
    });

    latch.await();
    Assert.assertEquals(callbackResponse.get(), future.get());
  }

  @Test
  public void testTimeoutOnGet() {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));

    testDataHubServer.getMockServer().reset();
    testDataHubServer.getMockServer()
        .when(request().withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal")
            .withHeader("Content-type", "application/json"), Times.once())
        .respond(org.mockserver.model.HttpResponse.response()
            .withStatusCode(200)
            .withDelay(TimeUnit.SECONDS, RestEmitterConfig.DEFAULT_READ_TIMEOUT_SEC + 3));

    MetadataChangeProposalWrapper mcpw = getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:foo");
    try {
      long startTime = System.currentTimeMillis();
      MetadataWriteResponse response = emitter.emit(mcpw, null).get();
      long duration = (long) ((System.currentTimeMillis() - startTime) / 1000.0);
      Assert.fail("Should not succeed with duration " + duration);
    } catch (Exception ioe) {
      Assert.assertTrue(ioe instanceof ExecutionException);
      Assert.assertTrue(((ExecutionException) ioe).getCause() instanceof SocketTimeoutException);
    }
  }

  @Test
  public void testTimeoutOnGetWithTimeout() {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));

    testDataHubServer.getMockServer().reset();
    testDataHubServer.getMockServer()
        .when(request().withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal")
            .withHeader("Content-type", "application/json"), Times.once())
        .respond(org.mockserver.model.HttpResponse.response()
            .withStatusCode(200)
            .withDelay(TimeUnit.SECONDS, RestEmitterConfig.DEFAULT_READ_TIMEOUT_SEC + 3));

    MetadataChangeProposalWrapper mcpw = getMetadataChangeProposalWrapper("Test Dataset", "urn:li:dataset:foo");
    try {
      long startTime = System.currentTimeMillis();
      MetadataWriteResponse response =
          emitter.emit(mcpw, null).get(RestEmitterConfig.DEFAULT_READ_TIMEOUT_SEC - 3, TimeUnit.SECONDS);
      long duration = (long) ((System.currentTimeMillis() - startTime) / 1000.0);
      Assert.fail("Should not succeed with duration " + duration);
    } catch (Exception ioe) {
      Assert.assertTrue(ioe instanceof TimeoutException);
    }
  }

  @Test
  public void testUserAgentHeader() throws IOException, ExecutionException, InterruptedException {
    TestDataHubServer testDataHubServer = new TestDataHubServer();
    Integer port = testDataHubServer.getMockServer().getPort();
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:" + port));
    testDataHubServer.getMockServer().reset();
    emitter.testConnection();
    Properties properties = new Properties();
    properties.load(emitter.getClass().getClassLoader().getResourceAsStream("client.properties"));
    Assert.assertNotNull(properties.getProperty("clientVersion"));
    String version = properties.getProperty("clientVersion");
    testDataHubServer.getMockServer().verify(
        request("/config")
            .withHeader("User-Agent", "DataHub-RestClient/" + version));
  }
}