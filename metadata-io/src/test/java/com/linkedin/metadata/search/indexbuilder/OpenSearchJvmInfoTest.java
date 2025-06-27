package com.linkedin.metadata.search.indexbuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.OpenSearchJvmInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.mockito.*;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OpenSearchJvmInfoTest {

  @Mock private RestHighLevelClient highLevelClient;

  @Mock private RestClient lowLevelClient;

  @Mock private Response response;

  @Mock private HttpEntity httpEntity;

  @Mock private StatusLine statusLine;

  private OpenSearchJvmInfo openSearchJvmInfo;
  private ObjectMapper objectMapper;

  @BeforeMethod
  void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(highLevelClient.getLowLevelClient()).thenReturn(lowLevelClient);
    openSearchJvmInfo = new OpenSearchJvmInfo(highLevelClient);
    objectMapper = new ObjectMapper();
  }

  @Test
  void testConstructor() {
    Assert.assertNotNull(openSearchJvmInfo);
    Mockito.verify(highLevelClient).getLowLevelClient();
  }

  @Test
  void testGetDataNodeJvmHeap_Success() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("data-node-1"));
    Assert.assertTrue(result.containsKey("data-node-2"));

    OpenSearchJvmInfo.JvmHeapInfo node1Info = result.get("data-node-1");
    Assert.assertEquals(node1Info.getHeapUsed(), 2147483648L); // 2GB in bytes
    Assert.assertEquals(node1Info.getHeapUsedPercent(), 75);
    Assert.assertEquals(node1Info.getHeapMax(), 4294967296L); // 4GB in bytes
    Assert.assertEquals(node1Info.getJvmVersion(), "11.0.2");

    // Verify the request was made correctly
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(lowLevelClient).performRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    Assert.assertEquals(capturedRequest.getMethod(), "GET");
    Assert.assertEquals(capturedRequest.getEndpoint(), "/_nodes/stats");
  }

  @Test
  void testGetDataNodeJvmHeap_EmptyResponse() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  void testGetDataNodeJvmHeap_NoNodesField() throws IOException {
    // Arrange
    String mockResponse = "{\"cluster_name\": \"test\"}";
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test(expectedExceptions = IOException.class)
  void testGetDataNodeJvmHeap_IOExceptionThrown() throws IOException {
    // Arrange
    Mockito.when(lowLevelClient.performRequest(ArgumentMatchers.any(Request.class)))
        .thenThrow(new IOException("Connection failed"));

    // Act & Assert
    openSearchJvmInfo.getDataNodeJvmHeap();
  }

  @Test
  void testGetSpecificDataNodeJvmHeap_Found() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.JvmHeapInfo result =
        openSearchJvmInfo.getSpecificDataNodeJvmHeap("data-node-1");

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getHeapUsed(), 2147483648L);
    Assert.assertEquals(result.getHeapUsedPercent(), 75);
  }

  @Test
  void testGetSpecificDataNodeJvmHeap_NotFound() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.JvmHeapInfo result =
        openSearchJvmInfo.getSpecificDataNodeJvmHeap("non-existent-node");

    // Assert
    Assert.assertNull(result);
  }

  @Test
  void testGetTotalDataNodesHeapCapacity() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    long result = openSearchJvmInfo.getTotalDataNodesHeapCapacity();

    // Assert
    Assert.assertEquals(result, 12884901888L); // 4GB + 8GB = 12GB in bytes
  }

  @Test
  void testGetTotalDataNodesHeapUsed() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    long result = openSearchJvmInfo.getTotalDataNodesHeapUsed();

    // Assert
    Assert.assertEquals(result, 6442450944L); // 2GB + 4GB = 6GB in bytes
  }

  @Test
  void testGetAverageDataNodesHeapUsagePercentage() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodesHeapUsagePercentage();

    // Assert
    Assert.assertEquals(result, 62.5, 0.01); // (75 + 50) / 2 = 62.5
  }

  @Test
  void testGetAverageDataNodeMaxHeapSize() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSize();

    // Assert
    Assert.assertEquals(result, 6442450944.0, 0.01); // (4GB + 8GB) / 2 = 6GB in bytes
  }

  @Test
  void testGetAverageDataNodeMaxHeapSizeGB() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSizeGB();

    // Assert
    Assert.assertEquals(result, 6.0, 0.01); // 6GB
  }

  @Test
  void testGetAverageDataNodeMaxHeapSize_EmptyNodes() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSize();

    // Assert
    Assert.assertEquals(result, 0.0);
  }

  @Test
  void testGetDataNodeHeapSizeStats() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.HeapSizeStats result = openSearchJvmInfo.getDataNodeHeapSizeStats();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getMinHeapBytes(), 4294967296L); // 4GB
    Assert.assertEquals(result.getMaxHeapBytes(), 8589934592L); // 8GB
    Assert.assertEquals(result.getAvgHeapBytes(), 6442450944.0, 0.01); // 6GB
    Assert.assertEquals(result.getTotalHeapBytes(), 12884901888L); // 12GB
    Assert.assertEquals(result.getNodeCount(), 2);
  }

  @Test
  void testGetDataNodeHeapSizeStats_EmptyNodes() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.HeapSizeStats result = openSearchJvmInfo.getDataNodeHeapSizeStats();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getMinHeapBytes(), 0);
    Assert.assertEquals(result.getMaxHeapBytes(), 0);
    Assert.assertEquals(result.getAvgHeapBytes(), 0.0);
    Assert.assertEquals(result.getTotalHeapBytes(), 0);
    Assert.assertEquals(result.getNodeCount(), 0);
  }

  @Test
  void testListDataNodesWithHeapDetails() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    List<OpenSearchJvmInfo.DataNodeInfo> result = openSearchJvmInfo.listDataNodesWithHeapDetails();

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);

    OpenSearchJvmInfo.DataNodeInfo node1 =
        result.stream()
            .filter(node -> "data-node-1".equals(node.getNodeName()))
            .findFirst()
            .orElse(null);

    Assert.assertNotNull(node1);
    Assert.assertEquals(node1.getNodeName(), "data-node-1");
    Assert.assertEquals(node1.getMaxHeapBytes(), 4294967296L);
    Assert.assertEquals(node1.getUsedHeapBytes(), 2147483648L);
    Assert.assertEquals(node1.getUsedHeapPercent(), 75);
    Assert.assertEquals(node1.getJvmVersion(), "11.0.2");
  }

  @Test
  void testJvmHeapInfo_Constructor() {
    // Act
    OpenSearchJvmInfo.JvmHeapInfo heapInfo =
        new OpenSearchJvmInfo.JvmHeapInfo(
            1000000000L,
            50,
            2000000000L,
            4000000000L,
            3600000L,
            "11.0.2",
            "OpenJDK",
            "Eclipse Adoptium");

    // Assert
    Assert.assertEquals(heapInfo.getHeapUsed(), 1000000000L);
    Assert.assertEquals(heapInfo.getHeapUsedPercent(), 50);
    Assert.assertEquals(heapInfo.getHeapCommitted(), 2000000000L);
    Assert.assertEquals(heapInfo.getHeapMax(), 4000000000L);
    Assert.assertEquals(heapInfo.getUptime(), 3600000L);
    Assert.assertEquals(heapInfo.getJvmVersion(), "11.0.2");
    Assert.assertEquals(heapInfo.getVmName(), "OpenJDK");
    Assert.assertEquals(heapInfo.getVmVendor(), "Eclipse Adoptium");
  }

  @Test
  void testJvmHeapInfo_GetHeapUsageFormatted() {
    // Arrange
    OpenSearchJvmInfo.JvmHeapInfo heapInfo =
        new OpenSearchJvmInfo.JvmHeapInfo(2147483648L, 50, 0L, 4294967296L, 0L, "", "", "");

    // Act
    String result = heapInfo.getHeapUsageFormatted();

    // Assert
    Assert.assertEquals(result, "2.00 GB / 4.00 GB (50%)");
  }

  @Test
  void testJvmHeapInfo_ToString() {
    // Arrange
    OpenSearchJvmInfo.JvmHeapInfo heapInfo =
        new OpenSearchJvmInfo.JvmHeapInfo(
            1073741824L, 25, 2147483648L, 4294967296L, 7200000L, "11.0.2", "OpenJDK", "Eclipse");

    // Act
    String result = heapInfo.toString();

    // Assert
    Assert.assertTrue(result.contains("heapUsed=1.0 GB"));
    Assert.assertTrue(result.contains("heapUsedPercent=25%"));
    Assert.assertTrue(result.contains("heapMax=4.0 GB"));
    Assert.assertTrue(result.contains("uptime=2 hours"));
    Assert.assertTrue(result.contains("jvmVersion='11.0.2'"));
  }

  @Test
  void testHeapSizeStats_Constructor() {
    // Act
    OpenSearchJvmInfo.HeapSizeStats stats =
        new OpenSearchJvmInfo.HeapSizeStats(
            2000000000L, 8000000000L, 4000000000.0, 12000000000L, 3);

    // Assert
    Assert.assertEquals(stats.getMinHeapBytes(), 2000000000L);
    Assert.assertEquals(stats.getMaxHeapBytes(), 8000000000L);
    Assert.assertEquals(stats.getAvgHeapBytes(), 4000000000.0);
    Assert.assertEquals(stats.getTotalHeapBytes(), 12000000000L);
    Assert.assertEquals(stats.getNodeCount(), 3);
  }

  @DataProvider(name = "heapSizeData")
  public Object[][] provideHeapSizeData() {
    return new Object[][] {
      {1073741824L, 1.0},
      {2147483648L, 2.0},
      {4294967296L, 4.0}
    };
  }

  @Test(dataProvider = "heapSizeData")
  void testHeapSizeStats_GetHeapGB(long bytes, double expectedGB) {
    // Arrange
    OpenSearchJvmInfo.HeapSizeStats stats =
        new OpenSearchJvmInfo.HeapSizeStats(bytes, bytes, bytes, bytes, 1);

    // Act & Assert
    Assert.assertEquals(stats.getMinHeapGB(), expectedGB, 0.01);
    Assert.assertEquals(stats.getMaxHeapGB(), expectedGB, 0.01);
    Assert.assertEquals(stats.getAvgHeapGB(), expectedGB, 0.01);
    Assert.assertEquals(stats.getTotalHeapGB(), expectedGB, 0.01);
  }

  @Test
  void testHeapSizeStats_ToString() {
    // Arrange
    OpenSearchJvmInfo.HeapSizeStats stats =
        new OpenSearchJvmInfo.HeapSizeStats(
            2147483648L, 8589934592L, 4294967296.0, 12884901888L, 3);

    // Act
    String result = stats.toString();

    // Assert
    Assert.assertTrue(result.contains("Heap Stats across 3 data nodes"));
    Assert.assertTrue(result.contains("Min: 2.00 GB"));
    Assert.assertTrue(result.contains("Max: 8.00 GB"));
    Assert.assertTrue(result.contains("Avg: 4.00 GB"));
    Assert.assertTrue(result.contains("Total: 12.00 GB"));
  }

  @Test
  void testDataNodeInfo_Constructor() {
    // Act
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo(
            "test-node", 4294967296L, 2147483648L, 50, "11.0.2", 3661000L);

    // Assert
    Assert.assertEquals(nodeInfo.getNodeName(), "test-node");
    Assert.assertEquals(nodeInfo.getMaxHeapBytes(), 4294967296L);
    Assert.assertEquals(nodeInfo.getUsedHeapBytes(), 2147483648L);
    Assert.assertEquals(nodeInfo.getUsedHeapPercent(), 50);
    Assert.assertEquals(nodeInfo.getJvmVersion(), "11.0.2");
    Assert.assertEquals(nodeInfo.getUptimeMillis(), 3661000L);
  }

  @DataProvider(name = "uptimeData")
  public Object[][] provideUptimeData() {
    return new Object[][] {
      {3600000L, "1 hours, 0 minutes"},
      {86400000L, "1 days, 0 hours"},
      {90061000L, "1 days, 1 hours"},
      {1800000L, "30 minutes"}
    };
  }

  @Test(dataProvider = "uptimeData")
  void testDataNodeInfo_GetUptimeFormatted(long uptimeMillis, String expectedFormat) {
    // Arrange
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo("test", 0L, 0L, 0, "", uptimeMillis);

    // Act
    String result = nodeInfo.getUptimeFormatted();

    // Assert
    Assert.assertEquals(result, expectedFormat);
  }

  @Test
  void testDataNodeInfo_GetHeapGB() {
    // Arrange
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo("test", 4294967296L, 2147483648L, 50, "", 0L);

    // Act & Assert
    Assert.assertEquals(nodeInfo.getMaxHeapGB(), 4.0, 0.01);
    Assert.assertEquals(nodeInfo.getUsedHeapGB(), 2.0, 0.01);
  }

  @Test
  void testDataNodeInfo_ToString() {
    // Arrange
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo(
            "test-node", 4294967296L, 2147483648L, 50, "11.0.2", 3600000L);

    // Act
    String result = nodeInfo.toString();

    // Assert
    Assert.assertTrue(result.contains("Node: test-node"));
    Assert.assertTrue(result.contains("Heap: 2.00 GB / 4.00 GB (50%)"));
    Assert.assertTrue(result.contains("JVM: 11.0.2"));
    Assert.assertTrue(result.contains("Uptime: 1 hours, 0 minutes"));
  }

  @Test
  void testFilterNonDataNodes() throws IOException {
    // Arrange
    String mockResponse = createMockResponseWithMasterNode();
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey("data-node-1"));
    Assert.assertFalse(result.containsKey("master-node-1"));
  }

  @Test
  void testNodeWithMissingJvmInfo() throws IOException {
    // Arrange
    String mockResponse = createMockResponseWithMissingJvm();
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assert.assertEquals(result.size(), 1); // Only the node with JVM info should be included
    Assert.assertTrue(result.containsKey("data-node-1"));
  }

  private void setupMockResponse(String responseBody) throws IOException {
    Mockito.when(lowLevelClient.performRequest(ArgumentMatchers.any(Request.class)))
        .thenReturn(response);
    Mockito.when(response.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent())
        .thenReturn(new java.io.ByteArrayInputStream(responseBody.getBytes()));
    // Mock EntityUtils.toString behavior
    Mockito.doAnswer(invocation -> responseBody).when(httpEntity).toString();
  }

  private String createMockNodesStatsResponse() {
    return "{\n"
        + "  \"nodes\": {\n"
        + "    \"node1\": {\n"
        + "      \"name\": \"data-node-1\",\n"
        + "      \"roles\": [\"data\", \"ingest\"],\n"
        + "      \"jvm\": {\n"
        + "        \"mem\": {\n"
        + "          \"heap_used_in_bytes\": 2147483648,\n"
        + "          \"heap_used_percent\": 75,\n"
        + "          \"heap_committed_in_bytes\": 3221225472,\n"
        + "          \"heap_max_in_bytes\": 4294967296\n"
        + "        },\n"
        + "        \"uptime_in_millis\": 7200000,\n"
        + "        \"version\": \"11.0.2\",\n"
        + "        \"vm_name\": \"OpenJDK 64-Bit Server VM\",\n"
        + "        \"vm_vendor\": \"Eclipse Adoptium\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"node2\": {\n"
        + "      \"name\": \"data-node-2\",\n"
        + "      \"roles\": [\"data\"],\n"
        + "      \"jvm\": {\n"
        + "        \"mem\": {\n"
        + "          \"heap_used_in_bytes\": 4294967296,\n"
        + "          \"heap_used_percent\": 50,\n"
        + "          \"heap_committed_in_bytes\": 6442450944,\n"
        + "          \"heap_max_in_bytes\": 8589934592\n"
        + "        },\n"
        + "        \"uptime_in_millis\": 3600000,\n"
        + "        \"version\": \"17.0.1\",\n"
        + "        \"vm_name\": \"OpenJDK 64-Bit Server VM\",\n"
        + "        \"vm_vendor\": \"Eclipse Adoptium\"\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  private String createMockResponseWithMasterNode() {
    return "{\n"
        + "  \"nodes\": {\n"
        + "    \"node1\": {\n"
        + "      \"name\": \"data-node-1\",\n"
        + "      \"roles\": [\"data\"],\n"
        + "      \"jvm\": {\n"
        + "        \"mem\": {\n"
        + "          \"heap_used_in_bytes\": 1073741824,\n"
        + "          \"heap_used_percent\": 25,\n"
        + "          \"heap_max_in_bytes\": 4294967296\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"node2\": {\n"
        + "      \"name\": \"master-node-1\",\n"
        + "      \"roles\": [\"master\"],\n"
        + "      \"jvm\": {\n"
        + "        \"mem\": {\n"
        + "          \"heap_used_in_bytes\": 536870912,\n"
        + "          \"heap_used_percent\": 25,\n"
        + "          \"heap_max_in_bytes\": 2147483648\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  private String createMockResponseWithMissingJvm() {
    return "{\n"
        + "  \"nodes\": {\n"
        + "    \"node1\": {\n"
        + "      \"name\": \"data-node-1\",\n"
        + "      \"roles\": [\"data\"],\n"
        + "      \"jvm\": {\n"
        + "        \"mem\": {\n"
        + "          \"heap_used_in_bytes\": 1073741824,\n"
        + "          \"heap_used_percent\": 25,\n"
        + "          \"heap_max_in_bytes\": 4294967296\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"node2\": {\n"
        + "      \"name\": \"data-node-2\",\n"
        + "      \"roles\": [\"data\"]\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }
}
