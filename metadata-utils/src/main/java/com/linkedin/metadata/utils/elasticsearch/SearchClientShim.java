package com.linkedin.metadata.utils.elasticsearch;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.arch.OperationContextExempt;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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
public interface SearchClientShim<T> extends Closeable, IndexSettingsComparison {

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

  @OperationContextExempt(reason = "Local accessor, no I/O.")
  ShimConfiguration getShimConfiguration();

  // Core search operations
  //
  // Data-plane methods take an {@link OperationFingerprint} as their first argument so the wrapper
  // layer (e.g. cloud's EnrichingShim) can hand it to its registered enrichers without changing
  // what the raw impls do. The raw impls (Es8/OpenSearch2/Es7Compatibility/FaultInjecting) ignore
  // the context — they remain pure adapters over the native client. Per-event routing, tenant
  // filtering, doc-field stamping, etc. is decoration applied at the wrapper layer.
  @Nonnull
  SearchResponse search(
      @Nonnull OperationFingerprint opContext,
      @Nonnull SearchRequest searchRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  SearchResponse scroll(
      @Nonnull OperationFingerprint opContext,
      @Nonnull SearchScrollRequest searchScrollRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  ClearScrollResponse clearScroll(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ClearScrollRequest clearScrollRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  CountResponse count(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CountRequest countRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  ExplainResponse explain(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ExplainRequest explainRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  // Document operations
  @Nonnull
  GetResponse getDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetRequest getRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  IndexResponse indexDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull IndexRequest indexRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  DeleteResponse deleteDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteRequest deleteRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  BulkByScrollResponse deleteByQuery(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteByQueryRequest deleteByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  CreatePitResponse createPit(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CreatePitRequest createPitRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  DeletePitResponse deletePit(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeletePitRequest deletePitRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  // Index management operations
  @Nonnull
  CreateIndexResponse createIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CreateIndexRequest createIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetIndexResponse getIndex(
      @Nonnull OperationFingerprint opContext,
      GetIndexRequest getIndexRequest,
      RequestOptions options)
      throws IOException;

  @Nonnull
  ResizeResponse cloneIndex(
      @Nonnull OperationFingerprint opContext, ResizeRequest resizeRequest, RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse deleteIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteIndexRequest deleteIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  boolean indexExists(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetIndexRequest getIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse putIndexMapping(
      @Nonnull OperationFingerprint opContext,
      @Nonnull PutMappingRequest putMappingRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetMappingsResponse getIndexMapping(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetMappingsRequest getMappingsRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetSettingsResponse getIndexSettings(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetSettingsRequest getSettingsRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse updateIndexSettings(
      @Nonnull OperationFingerprint opContext,
      @Nonnull UpdateSettingsRequest updateSettingsRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  RefreshResponse refreshIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull RefreshRequest refreshRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  GetAliasesResponse getIndexAliases(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetAliasesRequest getAliasesRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  AcknowledgedResponse updateIndexAliases(
      @Nonnull OperationFingerprint opContext,
      IndicesAliasesRequest indicesAliasesRequest,
      RequestOptions options)
      throws IOException;

  @Nonnull
  AnalyzeResponse analyzeIndex(
      @Nonnull OperationFingerprint opContext, AnalyzeRequest request, RequestOptions options)
      throws IOException;

  // Cluster operations
  @OperationContextExempt(
      reason =
          "Cluster-wide settings fetch; no tenant routing, no actor relevance, no per-request payload to enrich.")
  @Nonnull
  ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException;

  @OperationContextExempt(
      reason =
          "Cluster-wide settings update; no tenant routing, no actor relevance, no per-request payload to enrich.")
  @Nonnull
  ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException;

  @OperationContextExempt(
      reason = "Cluster-wide health probe — no tenant routing, no actor relevance")
  @Nonnull
  ClusterHealthResponse clusterHealth(ClusterHealthRequest healthRequest, RequestOptions options)
      throws IOException;

  // Async Task operations
  @OperationContextExempt(
      reason =
          "Cluster-wide task listing; no tenant routing, no actor relevance, no per-request payload to enrich.")
  @Nonnull
  ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options) throws IOException;

  @OperationContextExempt(
      reason =
          "Cluster task status probe — pure introspection; no tenant routing, no actor relevance.")
  @Nonnull
  Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException;

  /**
   * Like {@link #getTask} but also returns document failures from raw task JSON ({@code
   * response.failures[]}), which {@link GetTaskResponse} does not parse.
   *
   * <p>Default implementation has no raw JSON and returns an empty failure list. Engine-specific
   * shims override to preserve and parse the raw response.
   */
  @OperationContextExempt(
      reason =
          "Cluster task status probe — pure introspection; no tenant routing, no actor relevance.")
  @Nonnull
  default Optional<TaskResultWithFailures> getTaskWithFailures(
      GetTaskRequest request, RequestOptions options) throws IOException {
    return getTask(request, options).map(r -> new TaskResultWithFailures(r, List.of()));
  }

  // Metadata and introspection
  @OperationContextExempt(reason = "Local enum, no I/O.")
  @Nonnull
  SearchEngineType getEngineType();

  /**
   * Returns the base config for the {@code search_as_you_type} partial-ngram subfield.
   *
   * <p>ES7 and OpenSearch 2.x persist {@code doc_values: false} on round-trip, while ES8+ silently
   * strips it. Each shim implementation supplies the variant that matches its engine, keeping
   * engine-version knowledge inside the shim layer.
   */
  @OperationContextExempt(reason = "Static per-engine constant, no I/O.")
  @Nonnull
  Map<String, String> partialNgramConfig();

  @OperationContextExempt(
      reason = "Cluster version probe — pure introspection; no tenant routing, no actor relevance.")
  @Nonnull
  String getEngineVersion() throws IOException;

  @OperationContextExempt(
      reason =
          "Cluster metadata probe — pure introspection; no tenant routing, no actor relevance.")
  @Nonnull
  Map<String, String> getClusterInfo() throws IOException;

  /** Check if the client supports a specific API feature */
  @OperationContextExempt(reason = "Local feature flag, no I/O.")
  boolean supportsFeature(@Nonnull String feature);

  /**
   * WARNING: This breaks abstraction layers and should be considered an anti-pattern for usage,
   * likely candidate for removal.
   */
  @Nonnull
  RawResponse performLowLevelRequest(@Nonnull OperationFingerprint opContext, Request request)
      throws IOException;

  @Nonnull
  BulkByScrollResponse updateByQuery(
      @Nonnull OperationFingerprint opContext,
      @Nonnull UpdateByQueryRequest updateByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  String submitDeleteByQueryTask(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteByQueryRequest deleteByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  @Nonnull
  String submitReindexTask(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ReindexRequest reindexRequest,
      @Nonnull RequestOptions options)
      throws IOException;

  // Bulk operations
  @OperationContextExempt(reason = "Bulk processor lifecycle setup, not a per-event call.")
  void generateAsyncBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount);

  @OperationContextExempt(reason = "Bulk processor lifecycle setup, not a per-event call.")
  void generateBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount);

  void addBulk(
      @Nonnull OperationFingerprint opContext,
      @Nonnull String urn,
      @Nonnull DocWriteRequest<?> writeRequest);

  @OperationContextExempt(reason = "Bulk processor lifecycle, not a per-event call.")
  void flushBulkProcessor();

  @OperationContextExempt(reason = "Bulk processor lifecycle, not a per-event call.")
  void closeBulkProcessor();

  // -- Semantic search operations --------------------------------------------------

  default KnnSearchResponse searchKnn(
      @Nonnull OperationFingerprint opContext, @Nonnull KnnSearchRequest request)
      throws IOException {
    throw new UnsupportedOperationException(
        "searchKnn not supported by " + getEngineType() + " shim");
  }

  @OperationContextExempt(
      reason = "One-time index creation — infrastructure setup, no per-event tenant routing.")
  default void createSemanticIndex(@Nonnull SemanticIndexSpec spec) throws IOException {
    throw new UnsupportedOperationException(
        "createSemanticIndex not supported by " + getEngineType() + " shim");
  }

  default void indexEmbeddings(
      @Nonnull OperationFingerprint opContext, @Nonnull EmbeddingBatch batch) throws IOException {
    throw new UnsupportedOperationException(
        "indexEmbeddings not supported by " + getEngineType() + " shim");
  }

  /**
   * Get the native client instance for advanced operations that require direct access WARNING:
   * Using this breaks the abstraction and should be used sparingly
   */
  @OperationContextExempt(reason = "Escape-hatch accessor, no I/O.")
  @Nonnull
  T getNativeClient();
}
