package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.*;

/** Utility class to retrieve OpenSearch node JVM information via direct REST calls */
public class OpenSearchJvmInfo {

  private final RestHighLevelClient highLevelClient;
  private final RestClient lowLevelClient;
  private final ObjectMapper objectMapper;

  public OpenSearchJvmInfo(RestHighLevelClient highLevelClient) {
    this.highLevelClient = highLevelClient;
    this.lowLevelClient = highLevelClient.getLowLevelClient();
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Get JVM heap information for all data nodes in the OpenSearch cluster
   *
   * @return Map of node names to their JVM heap information
   * @throws IOException if there's an error communicating with the cluster
   */
  public Map<String, JvmHeapInfo> getDataNodeJvmHeap() throws IOException {
    Map<String, JvmHeapInfo> result = new HashMap<>();

    // Create direct request to _nodes endpoint with JVM and OS metrics
    Request request = new Request("GET", "/_nodes/stats");
    request.addParameter("filter_path", "nodes.*.jvm,nodes.*.roles");

    Response response = lowLevelClient.performRequest(request);
    HttpEntity entity = response.getEntity();
    String responseBody = EntityUtils.toString(entity);

    // Parse JSON response
    JsonNode rootNode = objectMapper.readTree(responseBody);
    JsonNode nodesNode = rootNode.get("nodes");

    if (nodesNode != null) {
      Iterator<Map.Entry<String, JsonNode>> nodes = nodesNode.fields();

      while (nodes.hasNext()) {
        Map.Entry<String, JsonNode> nodeEntry = nodes.next();
        String nodeId = nodeEntry.getKey();
        JsonNode nodeInfo = nodeEntry.getValue();

        // Check if this is a data node by checking roles
        JsonNode rolesNode = nodeInfo.get("roles");
        if (rolesNode != null && isDataNode(rolesNode)) {
          // Extract node name
          String nodeName = nodeInfo.has("name") ? nodeInfo.get("name").asText() : "node_" + nodeId;

          // Extract JVM info
          JsonNode jvmNode = nodeInfo.get("jvm");
          if (jvmNode != null) {
            JvmHeapInfo heapInfo = extractJvmHeapInfo(jvmNode);
            result.put(nodeName, heapInfo);
          }
        }
      }
    }

    return result;
  }

  /** Check if the node is a data node based on roles array */
  private boolean isDataNode(JsonNode rolesNode) {
    if (rolesNode.isArray()) {
      for (JsonNode role : rolesNode) {
        if (role.isTextual() && role.asText().equals("data")) {
          return true;
        }
      }
    }
    return false;
  }

  /** Extract JVM heap information from the JVM node */
  private JvmHeapInfo extractJvmHeapInfo(JsonNode jvmNode) {
    // Default values
    long heapUsed = 0;
    int heapUsedPercent = 0;
    long heapCommitted = 0;
    long heapMax = 0;
    long uptime = 0;
    String jvmVersion = "";
    String vmName = "";
    String vmVendor = "";

    // Extract mem info
    if (jvmNode.has("mem")) {
      JsonNode memNode = jvmNode.get("mem");

      if (memNode.has("heap_used_in_bytes")) {
        heapUsed = memNode.get("heap_used_in_bytes").asLong();
      }

      if (memNode.has("heap_used_percent")) {
        heapUsedPercent = memNode.get("heap_used_percent").asInt();
      }

      if (memNode.has("heap_committed_in_bytes")) {
        heapCommitted = memNode.get("heap_committed_in_bytes").asLong();
      }

      if (memNode.has("heap_max_in_bytes")) {
        heapMax = memNode.get("heap_max_in_bytes").asLong();
      }
    }

    // Extract uptime
    if (jvmNode.has("uptime_in_millis")) {
      uptime = jvmNode.get("uptime_in_millis").asLong();
    }

    // Extract version info
    if (jvmNode.has("version")) {
      jvmVersion = jvmNode.get("version").asText();
    }

    // Extract VM info
    if (jvmNode.has("vm_name")) {
      vmName = jvmNode.get("vm_name").asText();
    }

    if (jvmNode.has("vm_vendor")) {
      vmVendor = jvmNode.get("vm_vendor").asText();
    }

    return new JvmHeapInfo(
        heapUsed, heapUsedPercent, heapCommitted, heapMax, uptime, jvmVersion, vmName, vmVendor);
  }

  /**
   * Get detailed JVM heap information for a specific data node
   *
   * @param nodeName The name of the node to get information for
   * @return JVM heap information for the specified node, or null if not found
   * @throws IOException if there's an error communicating with the cluster
   */
  public JvmHeapInfo getSpecificDataNodeJvmHeap(String nodeName) throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();
    return allNodeHeapInfo.get(nodeName);
  }

  /**
   * Get total heap capacity across all data nodes
   *
   * @return The total maximum heap in bytes across all data nodes
   * @throws IOException if there's an error communicating with the cluster
   */
  public long getTotalDataNodesHeapCapacity() throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();

    return allNodeHeapInfo.values().stream().mapToLong(JvmHeapInfo::getHeapMax).sum();
  }

  /**
   * Get current total heap usage across all data nodes
   *
   * @return The total used heap in bytes across all data nodes
   * @throws IOException if there's an error communicating with the cluster
   */
  public long getTotalDataNodesHeapUsed() throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();

    return allNodeHeapInfo.values().stream().mapToLong(JvmHeapInfo::getHeapUsed).sum();
  }

  /**
   * Get average heap usage percentage across all data nodes
   *
   * @return The average heap usage percentage across all data nodes
   * @throws IOException if there's an error communicating with the cluster
   */
  public double getAverageDataNodesHeapUsagePercentage() throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();

    return allNodeHeapInfo.values().stream()
        .mapToInt(JvmHeapInfo::getHeapUsedPercent)
        .average()
        .orElse(0);
  }

  /**
   * Get the average maximum JVM heap size across all data nodes
   *
   * @return The average maximum heap size in bytes
   * @throws IOException if there's an error communicating with the cluster
   */
  public double getAverageDataNodeMaxHeapSize() throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();

    if (allNodeHeapInfo.isEmpty()) {
      return 0;
    }

    return allNodeHeapInfo.values().stream().mapToLong(JvmHeapInfo::getHeapMax).average().orElse(0);
  }

  /**
   * Get the average maximum JVM heap size across all data nodes in GB
   *
   * @return The average maximum heap size in gigabytes
   * @throws IOException if there's an error communicating with the cluster
   */
  public double getAverageDataNodeMaxHeapSizeGB() throws IOException {
    return bytesToGB(getAverageDataNodeMaxHeapSize());
  }

  /**
   * Get detailed stats about data node heap sizes
   *
   * @return A HeapSizeStats object containing min, max, avg, and total heap statistics
   * @throws IOException if there's an error communicating with the cluster
   */
  public HeapSizeStats getDataNodeHeapSizeStats() throws IOException {
    Map<String, JvmHeapInfo> allNodeHeapInfo = getDataNodeJvmHeap();

    if (allNodeHeapInfo.isEmpty()) {
      return new HeapSizeStats(0, 0, 0, 0, 0);
    }

    long minHeap = Long.MAX_VALUE;
    long maxHeap = 0;
    long totalHeap = 0;
    int nodeCount = 0;

    for (JvmHeapInfo heapInfo : allNodeHeapInfo.values()) {
      long heapMax = heapInfo.getHeapMax();

      if (heapMax > 0) { // Avoid counting nodes with unknown heap
        minHeap = Math.min(minHeap, heapMax);
        maxHeap = Math.max(maxHeap, heapMax);
        totalHeap += heapMax;
        nodeCount++;
      }
    }

    // If no valid nodes found, reset min to 0
    if (nodeCount == 0) {
      minHeap = 0;
    }

    double avgHeap = nodeCount > 0 ? (double) totalHeap / nodeCount : 0;

    return new HeapSizeStats(minHeap, maxHeap, avgHeap, totalHeap, nodeCount);
  }

  /**
   * List all data nodes with their heap details
   *
   * @return List of DataNodeInfo objects containing node details
   * @throws IOException if there's an error communicating with the cluster
   */
  public List<DataNodeInfo> listDataNodesWithHeapDetails() throws IOException {
    Map<String, JvmHeapInfo> heapInfoMap = getDataNodeJvmHeap();
    List<DataNodeInfo> result = new ArrayList<>();

    for (Map.Entry<String, JvmHeapInfo> entry : heapInfoMap.entrySet()) {
      String nodeName = entry.getKey();
      JvmHeapInfo heapInfo = entry.getValue();

      result.add(
          new DataNodeInfo(
              nodeName,
              heapInfo.getHeapMax(),
              heapInfo.getHeapUsed(),
              heapInfo.getHeapUsedPercent(),
              heapInfo.getJvmVersion(),
              heapInfo.getUptime()));
    }

    return result;
  }

  /** Utility method to convert bytes to gigabytes */
  private double bytesToGB(double bytes) {
    return bytes / (1024.0 * 1024.0 * 1024.0);
  }

  /** Class to hold JVM heap statistics */
  public static class JvmHeapInfo {
    private final long heapUsed;
    private final int heapUsedPercent;
    private final long heapCommitted;
    private final long heapMax;
    private final long uptime;
    private final String jvmVersion;
    private final String vmName;
    private final String vmVendor;

    public JvmHeapInfo(
        long heapUsed,
        int heapUsedPercent,
        long heapCommitted,
        long heapMax,
        long uptime,
        String jvmVersion,
        String vmName,
        String vmVendor) {
      this.heapUsed = heapUsed;
      this.heapUsedPercent = heapUsedPercent;
      this.heapCommitted = heapCommitted;
      this.heapMax = heapMax;
      this.uptime = uptime;
      this.jvmVersion = jvmVersion;
      this.vmName = vmName;
      this.vmVendor = vmVendor;
    }

    // Getters
    public long getHeapUsed() {
      return heapUsed;
    }

    public int getHeapUsedPercent() {
      return heapUsedPercent;
    }

    public long getHeapCommitted() {
      return heapCommitted;
    }

    public long getHeapMax() {
      return heapMax;
    }

    public long getUptime() {
      return uptime;
    }

    public String getJvmVersion() {
      return jvmVersion;
    }

    public String getVmName() {
      return vmName;
    }

    public String getVmVendor() {
      return vmVendor;
    }

    /**
     * Get heap usage as a human-readable string
     *
     * @return Formatted string with heap usage information
     */
    public String getHeapUsageFormatted() {
      return String.format(
          "%.2f GB / %.2f GB (%.2f%%)", bytesToGB(heapUsed), bytesToGB(heapMax), heapUsedPercent);
    }

    private double bytesToGB(long bytes) {
      return bytes / (1024.0 * 1024.0 * 1024.0);
    }

    @Override
    public String toString() {
      return "JvmHeapInfo{"
          + "heapUsed="
          + bytesToGB(heapUsed)
          + " GB"
          + ", heapUsedPercent="
          + heapUsedPercent
          + "%"
          + ", heapCommitted="
          + bytesToGB(heapCommitted)
          + " GB"
          + ", heapMax="
          + bytesToGB(heapMax)
          + " GB"
          + ", uptime="
          + (uptime / (1000 * 60 * 60))
          + " hours"
          + ", jvmVersion='"
          + jvmVersion
          + '\''
          + ", vmName='"
          + vmName
          + '\''
          + ", vmVendor='"
          + vmVendor
          + '\''
          + '}';
    }
  }

  /** Class to hold comprehensive heap size statistics */
  public static class HeapSizeStats {
    private final long minHeapBytes;
    private final long maxHeapBytes;
    private final double avgHeapBytes;
    private final long totalHeapBytes;
    private final int nodeCount;

    public HeapSizeStats(
        long minHeapBytes,
        long maxHeapBytes,
        double avgHeapBytes,
        long totalHeapBytes,
        int nodeCount) {
      this.minHeapBytes = minHeapBytes;
      this.maxHeapBytes = maxHeapBytes;
      this.avgHeapBytes = avgHeapBytes;
      this.totalHeapBytes = totalHeapBytes;
      this.nodeCount = nodeCount;
    }

    // Getters
    public long getMinHeapBytes() {
      return minHeapBytes;
    }

    public long getMaxHeapBytes() {
      return maxHeapBytes;
    }

    public double getAvgHeapBytes() {
      return avgHeapBytes;
    }

    public long getTotalHeapBytes() {
      return totalHeapBytes;
    }

    public int getNodeCount() {
      return nodeCount;
    }

    public double getMinHeapGB() {
      return minHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public double getMaxHeapGB() {
      return maxHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public double getAvgHeapGB() {
      return avgHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public double getTotalHeapGB() {
      return totalHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    @Override
    public String toString() {
      return String.format(
          "Heap Stats across %d data nodes:%n"
              + "  Min: %.2f GB%n"
              + "  Max: %.2f GB%n"
              + "  Avg: %.2f GB%n"
              + "  Total: %.2f GB",
          nodeCount, getMinHeapGB(), getMaxHeapGB(), getAvgHeapGB(), getTotalHeapGB());
    }
  }

  /** Class to hold data node information for listing */
  public static class DataNodeInfo {
    private final String nodeName;
    private final long maxHeapBytes;
    private final long usedHeapBytes;
    private final int usedHeapPercent;
    private final String jvmVersion;
    private final long uptimeMillis;

    public DataNodeInfo(
        String nodeName,
        long maxHeapBytes,
        long usedHeapBytes,
        int usedHeapPercent,
        String jvmVersion,
        long uptimeMillis) {
      this.nodeName = nodeName;
      this.maxHeapBytes = maxHeapBytes;
      this.usedHeapBytes = usedHeapBytes;
      this.usedHeapPercent = usedHeapPercent;
      this.jvmVersion = jvmVersion;
      this.uptimeMillis = uptimeMillis;
    }

    // Getters
    public String getNodeName() {
      return nodeName;
    }

    public long getMaxHeapBytes() {
      return maxHeapBytes;
    }

    public long getUsedHeapBytes() {
      return usedHeapBytes;
    }

    public int getUsedHeapPercent() {
      return usedHeapPercent;
    }

    public String getJvmVersion() {
      return jvmVersion;
    }

    public long getUptimeMillis() {
      return uptimeMillis;
    }

    public double getMaxHeapGB() {
      return maxHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public double getUsedHeapGB() {
      return usedHeapBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public String getUptimeFormatted() {
      long seconds = uptimeMillis / 1000;
      long minutes = seconds / 60;
      long hours = minutes / 60;
      long days = hours / 24;

      if (days > 0) {
        return String.format("%d days, %d hours", days, hours % 24);
      } else if (hours > 0) {
        return String.format("%d hours, %d minutes", hours, minutes % 60);
      } else {
        return String.format("%d minutes", minutes);
      }
    }

    @Override
    public String toString() {
      return String.format(
          "Node: %s, Heap: %.2f GB / %.2f GB (%.2f%%), JVM: %s, Uptime: %s",
          nodeName,
          getUsedHeapGB(),
          getMaxHeapGB(),
          usedHeapPercent,
          jvmVersion,
          getUptimeFormatted());
    }
  }

  /** Example usage */
  public static void main(String[] args) {
    // Example client initialization
    // RestHighLevelClient client = new RestHighLevelClient(
    //     RestClient.builder(new HttpHost("localhost", 9200, "http")));
    //
    // OpenSearchJvmInfo jvmInfo = new OpenSearchJvmInfo(client);
    //
    // try {
    //     // Get JVM heap info for all data nodes
    //     Map<String, JvmHeapInfo> allNodesHeap = jvmInfo.getDataNodeJvmHeap();
    //     System.out.println("JVM Heap info for all data nodes:");
    //     allNodesHeap.forEach((node, heap) -> {
    //         System.out.println(node + ": " + heap.getHeapUsageFormatted());
    //     });
    //
    //     // Get total heap capacity
    //     long totalHeapBytes = jvmInfo.getTotalDataNodesHeapCapacity();
    //     double totalHeapGB = totalHeapBytes / (1024.0 * 1024.0 * 1024.0);
    //     System.out.println("Total heap capacity across all data nodes: " +
    //             String.format("%.2f GB", totalHeapGB));
    //
    //     // Get average heap usage percentage
    //     double avgHeapUsage = jvmInfo.getAverageDataNodesHeapUsagePercentage();
    //     System.out.println("Average heap usage across all data nodes: " +
    //             String.format("%.2f%%", avgHeapUsage));
    //
    //     // Get average max heap size
    //     double avgMaxHeapGB = jvmInfo.getAverageDataNodeMaxHeapSizeGB();
    //     System.out.println("Average maximum heap size per data node: " +
    //             String.format("%.2f GB", avgMaxHeapGB));
    //
    //     // Get comprehensive heap statistics
    //     HeapSizeStats heapStats = jvmInfo.getDataNodeHeapSizeStats();
    //     System.out.println(heapStats);
    //
    //     // List all data nodes with heap details
    //     List<DataNodeInfo> nodeList = jvmInfo.listDataNodesWithHeapDetails();
    //     System.out.println("\nData nodes with heap details:");
    //     nodeList.forEach(System.out::println);
    //
    // } catch (IOException e) {
    //     e.printStackTrace();
    // } finally {
    //     try {
    //         client.close();
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }
  }
}
