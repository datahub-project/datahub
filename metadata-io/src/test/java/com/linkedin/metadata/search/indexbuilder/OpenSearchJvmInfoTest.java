package com.linkedin.metadata.search.indexbuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.OpenSearchJvmInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.*;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

public class OpenSearchJvmInfoTest {

  @Mock private RestHighLevelClient highLevelClient;

  @Mock private RestClient lowLevelClient;

  @Mock private Response response;

  @Mock private HttpEntity httpEntity;

  @Mock private StatusLine statusLine;

  private OpenSearchJvmInfo openSearchJvmInfo;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(highLevelClient.getLowLevelClient()).thenReturn(lowLevelClient);
    openSearchJvmInfo = new OpenSearchJvmInfo(highLevelClient);
    objectMapper = new ObjectMapper();
  }

  @Test
  void testConstructor() {
    Assertions.assertNotNull(openSearchJvmInfo);
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.containsKey("data-node-1"));
    Assertions.assertTrue(result.containsKey("data-node-2"));

    OpenSearchJvmInfo.JvmHeapInfo node1Info = result.get("data-node-1");
    Assertions.assertEquals(2147483648L, node1Info.getHeapUsed()); // 2GB in bytes
    Assertions.assertEquals(75, node1Info.getHeapUsedPercent());
    Assertions.assertEquals(4294967296L, node1Info.getHeapMax()); // 4GB in bytes
    Assertions.assertEquals("11.0.2", node1Info.getJvmVersion());

    // Verify the request was made correctly
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(lowLevelClient).performRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    Assertions.assertEquals("GET", capturedRequest.getMethod());
    Assertions.assertEquals("/_nodes/stats", capturedRequest.getEndpoint());
  }

  @Test
  void testGetDataNodeJvmHeap_EmptyResponse() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testGetDataNodeJvmHeap_NoNodesField() throws IOException {
    // Arrange
    String mockResponse = "{\"cluster_name\": \"test\"}";
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testGetDataNodeJvmHeap_IOExceptionThrown() throws IOException {
    // Arrange
    Mockito.when(lowLevelClient.performRequest(ArgumentMatchers.any(Request.class)))
        .thenThrow(new IOException("Connection failed"));

    // Act & Assert
    Assertions.assertThrows(IOException.class, () -> openSearchJvmInfo.getDataNodeJvmHeap());
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals(2147483648L, result.getHeapUsed());
    Assertions.assertEquals(75, result.getHeapUsedPercent());
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
    Assertions.assertNull(result);
  }

  @Test
  void testGetTotalDataNodesHeapCapacity() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    long result = openSearchJvmInfo.getTotalDataNodesHeapCapacity();

    // Assert
    Assertions.assertEquals(12884901888L, result); // 4GB + 8GB = 12GB in bytes
  }

  @Test
  void testGetTotalDataNodesHeapUsed() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    long result = openSearchJvmInfo.getTotalDataNodesHeapUsed();

    // Assert
    Assertions.assertEquals(6442450944L, result); // 2GB + 4GB = 6GB in bytes
  }

  @Test
  void testGetAverageDataNodesHeapUsagePercentage() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodesHeapUsagePercentage();

    // Assert
    Assertions.assertEquals(62.5, result, 0.01); // (75 + 50) / 2 = 62.5
  }

  @Test
  void testGetAverageDataNodeMaxHeapSize() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSize();

    // Assert
    Assertions.assertEquals(6442450944.0, result, 0.01); // (4GB + 8GB) / 2 = 6GB in bytes
  }

  @Test
  void testGetAverageDataNodeMaxHeapSizeGB() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSizeGB();

    // Assert
    Assertions.assertEquals(6.0, result, 0.01); // 6GB
  }

  @Test
  void testGetAverageDataNodeMaxHeapSize_EmptyNodes() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    double result = openSearchJvmInfo.getAverageDataNodeMaxHeapSize();

    // Assert
    Assertions.assertEquals(0.0, result);
  }

  @Test
  void testGetDataNodeHeapSizeStats() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.HeapSizeStats result = openSearchJvmInfo.getDataNodeHeapSizeStats();

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals(4294967296L, result.getMinHeapBytes()); // 4GB
    Assertions.assertEquals(8589934592L, result.getMaxHeapBytes()); // 8GB
    Assertions.assertEquals(6442450944.0, result.getAvgHeapBytes(), 0.01); // 6GB
    Assertions.assertEquals(12884901888L, result.getTotalHeapBytes()); // 12GB
    Assertions.assertEquals(2, result.getNodeCount());
  }

  @Test
  void testGetDataNodeHeapSizeStats_EmptyNodes() throws IOException {
    // Arrange
    String mockResponse = "{\"nodes\": {}}";
    setupMockResponse(mockResponse);

    // Act
    OpenSearchJvmInfo.HeapSizeStats result = openSearchJvmInfo.getDataNodeHeapSizeStats();

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals(0, result.getMinHeapBytes());
    Assertions.assertEquals(0, result.getMaxHeapBytes());
    Assertions.assertEquals(0.0, result.getAvgHeapBytes());
    Assertions.assertEquals(0, result.getTotalHeapBytes());
    Assertions.assertEquals(0, result.getNodeCount());
  }

  @Test
  void testListDataNodesWithHeapDetails() throws IOException {
    // Arrange
    String mockResponse = createMockNodesStatsResponse();
    setupMockResponse(mockResponse);

    // Act
    List<OpenSearchJvmInfo.DataNodeInfo> result = openSearchJvmInfo.listDataNodesWithHeapDetails();

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());

    OpenSearchJvmInfo.DataNodeInfo node1 =
        result.stream()
            .filter(node -> "data-node-1".equals(node.getNodeName()))
            .findFirst()
            .orElse(null);

    Assertions.assertNotNull(node1);
    Assertions.assertEquals("data-node-1", node1.getNodeName());
    Assertions.assertEquals(4294967296L, node1.getMaxHeapBytes());
    Assertions.assertEquals(2147483648L, node1.getUsedHeapBytes());
    Assertions.assertEquals(75, node1.getUsedHeapPercent());
    Assertions.assertEquals("11.0.2", node1.getJvmVersion());
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
    Assertions.assertEquals(1000000000L, heapInfo.getHeapUsed());
    Assertions.assertEquals(50, heapInfo.getHeapUsedPercent());
    Assertions.assertEquals(2000000000L, heapInfo.getHeapCommitted());
    Assertions.assertEquals(4000000000L, heapInfo.getHeapMax());
    Assertions.assertEquals(3600000L, heapInfo.getUptime());
    Assertions.assertEquals("11.0.2", heapInfo.getJvmVersion());
    Assertions.assertEquals("OpenJDK", heapInfo.getVmName());
    Assertions.assertEquals("Eclipse Adoptium", heapInfo.getVmVendor());
  }

  @Test
  void testJvmHeapInfo_GetHeapUsageFormatted() {
    // Arrange
    OpenSearchJvmInfo.JvmHeapInfo heapInfo =
        new OpenSearchJvmInfo.JvmHeapInfo(2147483648L, 50, 0L, 4294967296L, 0L, "", "", "");

    // Act
    String result = heapInfo.getHeapUsageFormatted();

    // Assert
    Assertions.assertEquals("2.00 GB / 4.00 GB (50.00%)", result);
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
    Assertions.assertTrue(result.contains("heapUsed=1.0 GB"));
    Assertions.assertTrue(result.contains("heapUsedPercent=25%"));
    Assertions.assertTrue(result.contains("heapMax=4.0 GB"));
    Assertions.assertTrue(result.contains("uptime=2 hours"));
    Assertions.assertTrue(result.contains("jvmVersion='11.0.2'"));
  }

  @Test
  void testHeapSizeStats_Constructor() {
    // Act
    OpenSearchJvmInfo.HeapSizeStats stats =
        new OpenSearchJvmInfo.HeapSizeStats(
            2000000000L, 8000000000L, 4000000000.0, 12000000000L, 3);

    // Assert
    Assertions.assertEquals(2000000000L, stats.getMinHeapBytes());
    Assertions.assertEquals(8000000000L, stats.getMaxHeapBytes());
    Assertions.assertEquals(4000000000.0, stats.getAvgHeapBytes());
    Assertions.assertEquals(12000000000L, stats.getTotalHeapBytes());
    Assertions.assertEquals(3, stats.getNodeCount());
  }

  @ParameterizedTest
  @CsvSource({"1073741824, 1.0", "2147483648, 2.0", "4294967296, 4.0"})
  void testHeapSizeStats_GetHeapGB(long bytes, double expectedGB) {
    // Arrange
    OpenSearchJvmInfo.HeapSizeStats stats =
        new OpenSearchJvmInfo.HeapSizeStats(bytes, bytes, bytes, bytes, 1);

    // Act & Assert
    Assertions.assertEquals(expectedGB, stats.getMinHeapGB(), 0.01);
    Assertions.assertEquals(expectedGB, stats.getMaxHeapGB(), 0.01);
    Assertions.assertEquals(expectedGB, stats.getAvgHeapGB(), 0.01);
    Assertions.assertEquals(expectedGB, stats.getTotalHeapGB(), 0.01);
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
    Assertions.assertTrue(result.contains("Heap Stats across 3 data nodes"));
    Assertions.assertTrue(result.contains("Min: 2.00 GB"));
    Assertions.assertTrue(result.contains("Max: 8.00 GB"));
    Assertions.assertTrue(result.contains("Avg: 4.00 GB"));
    Assertions.assertTrue(result.contains("Total: 12.00 GB"));
  }

  @Test
  void testDataNodeInfo_Constructor() {
    // Act
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo(
            "test-node", 4294967296L, 2147483648L, 50, "11.0.2", 3661000L);

    // Assert
    Assertions.assertEquals("test-node", nodeInfo.getNodeName());
    Assertions.assertEquals(4294967296L, nodeInfo.getMaxHeapBytes());
    Assertions.assertEquals(2147483648L, nodeInfo.getUsedHeapBytes());
    Assertions.assertEquals(50, nodeInfo.getUsedHeapPercent());
    Assertions.assertEquals("11.0.2", nodeInfo.getJvmVersion());
    Assertions.assertEquals(3661000L, nodeInfo.getUptimeMillis());
  }

  @ParameterizedTest
  @CsvSource({
    "3600000, '1 hours, 0 minutes'",
    "86400000, '1 days, 0 hours'",
    "90061000, '1 days, 1 hours'",
    "1800000, '30 minutes'"
  })
  void testDataNodeInfo_GetUptimeFormatted(long uptimeMillis, String expectedFormat) {
    // Arrange
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo("test", 0L, 0L, 0, "", uptimeMillis);

    // Act
    String result = nodeInfo.getUptimeFormatted();

    // Assert
    Assertions.assertEquals(expectedFormat, result);
  }

  @Test
  void testDataNodeInfo_GetHeapGB() {
    // Arrange
    OpenSearchJvmInfo.DataNodeInfo nodeInfo =
        new OpenSearchJvmInfo.DataNodeInfo("test", 4294967296L, 2147483648L, 50, "", 0L);

    // Act & Assert
    Assertions.assertEquals(4.0, nodeInfo.getMaxHeapGB(), 0.01);
    Assertions.assertEquals(2.0, nodeInfo.getUsedHeapGB(), 0.01);
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
    Assertions.assertTrue(result.contains("Node: test-node"));
    Assertions.assertTrue(result.contains("Heap: 2.00 GB / 4.00 GB (50%)"));
    Assertions.assertTrue(result.contains("JVM: 11.0.2"));
    Assertions.assertTrue(result.contains("Uptime: 1 hours, 0 minutes"));
  }

  @Test
  void testFilterNonDataNodes() throws IOException {
    // Arrange
    String mockResponse = createMockResponseWithMasterNode();
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assertions.assertEquals(1, result.size());
    Assertions.assertTrue(result.containsKey("data-node-1"));
    Assertions.assertFalse(result.containsKey("master-node-1"));
  }

  @Test
  void testNodeWithMissingJvmInfo() throws IOException {
    // Arrange
    String mockResponse = createMockResponseWithMissingJvm();
    setupMockResponse(mockResponse);

    // Act
    Map<String, OpenSearchJvmInfo.JvmHeapInfo> result = openSearchJvmInfo.getDataNodeJvmHeap();

    // Assert
    Assertions.assertEquals(1, result.size()); // Only the node with JVM info should be included
    Assertions.assertTrue(result.containsKey("data-node-1"));
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
