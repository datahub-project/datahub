package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.indices.AnalyzeResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.client.indices.ResizeResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;

/**
 * Shim interface that abstracts different Elasticsearch/OpenSearch client implementations. This
 * allows DataHub to support ES 7.17 (with API compatibility), ES 8.x, ES 9.x and OpenSearch 2.x.,
 * through a common interface.
 */
public interface SearchClientShim<T> extends Closeable {

  /** Enum representing the different search engine types supported by the shim */
  enum SearchEngineType {
    ELASTICSEARCH_7("elasticsearch", "7"),
    ELASTICSEARCH_8("elasticsearch", "8"),
    ELASTICSEARCH_9("elasticsearch", "9"),
    OPENSEARCH_2("opensearch", "2"),
    UNKNOWN("unsupported", "none");

    private final String engine;
    private final String majorVersion;

    SearchEngineType(String engine, String majorVersion) {
      this.engine = engine;
      this.majorVersion = majorVersion;
    }

    public String getEngine() {
      return engine;
    }

    public String getMajorVersion() {
      return majorVersion;
    }

    public boolean isElasticsearch() {
      return "elasticsearch".equals(engine);
    }

    public boolean isOpenSearch() {
      return "opensearch".equals(engine);
    }

    /** Determine if this engine type supports the ES 7.x REST high-level client */
    public boolean supportsEs7HighLevelClient() {
      return this == ELASTICSEARCH_7 || this == OPENSEARCH_2;
    }

    /** Determine if this engine type requires the new ES 8.x Java client */
    public boolean requiresEs8JavaClient() {
      return this == ELASTICSEARCH_8 || this == ELASTICSEARCH_9;
    }

    /** Determine if this engine type requires OpenSearch specific client */
    public boolean requiresOpenSearchClient() {
      return false; // OpenSearch 3.x support not yet implemented
    }
  }

  /** Configuration for the shim client */
  interface ShimConfiguration {
    SearchEngineType getEngineType();

    String getHost();

    Integer getPort();

    String getUsername();

    String getPassword();

    boolean isUseSSL();

    String getPathPrefix();

    boolean isUseAwsIamAuth();

    String getRegion();

    Integer getThreadCount();

    Integer getConnectionRequestTimeout();

    Integer getSocketTimeout();

    SSLContext getSSLContext();
  }

  ShimConfiguration getShimConfiguration();

  // Core search operations
  @Nonnull
  SearchResponse search(@Nonnull SearchRequest searchRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  SearchResponse scroll(
      @Nonnull SearchScrollRequest searchScrollRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  ClearScrollResponse clearScroll(
      @Nonnull ClearScrollRequest clearScrollRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  CountResponse count(@Nonnull CountRequest countRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  ExplainResponse explain(@Nonnull ExplainRequest explainRequest, @Nonnull RequestOptions options)
      throws IOException;

  // Document operations
  @Nonnull
  GetResponse getDocument(@Nonnull GetRequest getRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  IndexResponse indexDocument(@Nonnull IndexRequest indexRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  DeleteResponse deleteDocument(
      @Nonnull DeleteRequest deleteRequest, @Nonnull RequestOptions options) throws IOException;

  @Nonnull
  BulkByScrollResponse deleteByQuery(
      @Nonnull DeleteByQueryRequest deleteByQueryRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  CreatePitResponse createPit(
      @Nonnull CreatePitRequest createPitRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  DeletePitResponse deletePit(
      @Nonnull DeletePitRequest deletePitRequest, @Nonnull RequestOptions options)
      throws IOException;

  // Index management operations
  @Nonnull
  CreateIndexResponse createIndex(
      @Nonnull CreateIndexRequest createIndexRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options)
      throws IOException;

  @Nonnull
  ResizeResponse cloneIndex(ResizeRequest resizeRequest, RequestOptions options) throws IOException;

  @Nonnull
  AcknowledgedResponse deleteIndex(
      @Nonnull DeleteIndexRequest deleteIndexRequest, @Nonnull RequestOptions options)
      throws IOException;

  boolean indexExists(@Nonnull GetIndexRequest getIndexRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse putIndexMapping(
      @Nonnull PutMappingRequest putMappingRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetMappingsResponse getIndexMapping(
      @Nonnull GetMappingsRequest getMappingsRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetSettingsResponse getIndexSettings(
      @Nonnull GetSettingsRequest getSettingsRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse updateIndexSettings(
      @Nonnull UpdateSettingsRequest updateSettingsRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  RefreshResponse refreshIndex(
      @Nonnull RefreshRequest refreshRequest, @Nonnull RequestOptions options) throws IOException;

  @Nonnull
  GetAliasesResponse getIndexAliases(
      @Nonnull GetAliasesRequest getAliasesRequest, @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse updateIndexAliases(
      IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException;

  @Nonnull
  AnalyzeResponse analyzeIndex(AnalyzeRequest request, RequestOptions options) throws IOException;

  // Cluster operations
  @Nonnull
  ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException;

  @Nonnull
  ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException;

  @Nonnull
  ClusterHealthResponse clusterHealth(ClusterHealthRequest healthRequest, RequestOptions options)
      throws IOException;

  // Async Task operations
  @Nonnull
  ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options) throws IOException;

  @Nonnull
  Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException;

  // Metadata and introspection
  @Nonnull
  SearchEngineType getEngineType();

  @Nonnull
  String getEngineVersion() throws IOException;

  @Nonnull
  Map<String, String> getClusterInfo() throws IOException;

  /** Check if the client supports a specific API feature */
  boolean supportsFeature(@Nonnull String feature);

  /**
   * WARNING: This breaks abstraction layers and should be considered an anti-pattern for usage,
   * likely candidate for removal.
   */
  @Nonnull
  RawResponse performLowLevelRequest(Request request) throws IOException;

  @Nonnull
  BulkByScrollResponse updateByQuery(
      UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException;

  @Nonnull
  String submitDeleteByQueryTask(DeleteByQueryRequest deleteByQueryRequest, RequestOptions options)
      throws IOException;

  @Nonnull
  String submitReindexTask(ReindexRequest reindexRequest, RequestOptions options)
      throws IOException;

  // Bulk operations
  void generateAsyncBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount);

  void generateBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount);

  void addBulk(DocWriteRequest<?> writeRequest);

  void addBulk(String urn, DocWriteRequest<?> writeRequest);

  void flushBulkProcessor();

  void closeBulkProcessor();

  /**
   * Get the native client instance for advanced operations that require direct access WARNING:
   * Using this breaks the abstraction and should be used sparingly
   */
  @Nonnull
  T getNativeClient();
}
