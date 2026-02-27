package io.datahubproject.test.search;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
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
 * Test-only decorator that wraps a real SearchClientShim and optionally throws on the first N
 * {@code count()} or {@code createIndex()} calls, then delegates. Used to exercise retry logic in
 * ESIndexBuilder against real containers. The fault spec is read from {@link FaultSpec.Holder} so
 * tests can set it per test (e.g. in @BeforeMethod).
 */
public class FaultInjectingSearchClientShim implements SearchClientShim<Object> {

  private final SearchClientShim<?> delegate;
  private final AtomicInteger countInvocations = new AtomicInteger(0);
  private final AtomicInteger createIndexInvocations = new AtomicInteger(0);

  public FaultInjectingSearchClientShim(@Nonnull SearchClientShim<?> delegate) {
    this.delegate = delegate;
  }

  /** Exposed for test configuration (e.g. bulk processor must use the real shim). */
  @Nonnull
  public SearchClientShim<?> getDelegate() {
    return delegate;
  }

  @Override
  @Nonnull
  public CountResponse count(@Nonnull CountRequest countRequest, @Nonnull RequestOptions options)
      throws IOException {
    FaultSpec spec = FaultSpec.Holder.get();
    if (spec != null
        && spec.getCountFailures() > 0
        && countInvocations.getAndIncrement() < spec.getCountFailures()) {
      Exception ex = spec.createCountException();
      if (ex instanceof IOException) {
        throw (IOException) ex;
      }
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new IOException(ex);
    }
    return delegate.count(countRequest, options);
  }

  @Override
  @Nonnull
  public CreateIndexResponse createIndex(
      @Nonnull CreateIndexRequest createIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    FaultSpec spec = FaultSpec.Holder.get();
    if (spec != null
        && spec.getCreateIndexFailures() > 0
        && createIndexInvocations.getAndIncrement() < spec.getCreateIndexFailures()) {
      throw new IOException("simulated createIndex failure for test");
    }
    return delegate.createIndex(createIndexRequest, options);
  }

  @Override
  public SearchClientShim.ShimConfiguration getShimConfiguration() {
    return delegate.getShimConfiguration();
  }

  @Override
  @Nonnull
  public SearchResponse search(
      @Nonnull SearchRequest searchRequest, @Nonnull RequestOptions options) throws IOException {
    return delegate.search(searchRequest, options);
  }

  @Override
  @Nonnull
  public SearchResponse scroll(
      @Nonnull SearchScrollRequest searchScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.scroll(searchScrollRequest, options);
  }

  @Override
  @Nonnull
  public ClearScrollResponse clearScroll(
      @Nonnull ClearScrollRequest clearScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.clearScroll(clearScrollRequest, options);
  }

  @Override
  @Nonnull
  public ExplainResponse explain(
      @Nonnull ExplainRequest explainRequest, @Nonnull RequestOptions options) throws IOException {
    return delegate.explain(explainRequest, options);
  }

  @Override
  @Nonnull
  public GetResponse getDocument(@Nonnull GetRequest getRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.getDocument(getRequest, options);
  }

  @Override
  @Nonnull
  public IndexResponse indexDocument(
      @Nonnull IndexRequest indexRequest, @Nonnull RequestOptions options) throws IOException {
    return delegate.indexDocument(indexRequest, options);
  }

  @Override
  @Nonnull
  public DeleteResponse deleteDocument(
      @Nonnull DeleteRequest deleteRequest, @Nonnull RequestOptions options) throws IOException {
    return delegate.deleteDocument(deleteRequest, options);
  }

  @Override
  @Nonnull
  public BulkByScrollResponse deleteByQuery(
      @Nonnull DeleteByQueryRequest deleteByQueryRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.deleteByQuery(deleteByQueryRequest, options);
  }

  @Override
  @Nonnull
  public CreatePitResponse createPit(
      @Nonnull CreatePitRequest createPitRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.createPit(createPitRequest, options);
  }

  @Override
  @Nonnull
  public DeletePitResponse deletePit(
      @Nonnull DeletePitRequest deletePitRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.deletePit(deletePitRequest, options);
  }

  @Override
  @Nonnull
  public GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options)
      throws IOException {
    return delegate.getIndex(getIndexRequest, options);
  }

  @Override
  @Nonnull
  public ResizeResponse cloneIndex(ResizeRequest resizeRequest, RequestOptions options)
      throws IOException {
    return delegate.cloneIndex(resizeRequest, options);
  }

  @Override
  @Nonnull
  public AcknowledgedResponse deleteIndex(
      @Nonnull DeleteIndexRequest deleteIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.deleteIndex(deleteIndexRequest, options);
  }

  @Override
  public boolean indexExists(
      @Nonnull GetIndexRequest getIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.indexExists(getIndexRequest, options);
  }

  @Override
  @Nonnull
  public AcknowledgedResponse putIndexMapping(
      @Nonnull PutMappingRequest putMappingRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.putIndexMapping(putMappingRequest, options);
  }

  @Override
  @Nonnull
  public GetMappingsResponse getIndexMapping(
      @Nonnull GetMappingsRequest getMappingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.getIndexMapping(getMappingsRequest, options);
  }

  @Override
  @Nonnull
  public GetSettingsResponse getIndexSettings(
      @Nonnull GetSettingsRequest getSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.getIndexSettings(getSettingsRequest, options);
  }

  @Override
  @Nonnull
  public AcknowledgedResponse updateIndexSettings(
      @Nonnull UpdateSettingsRequest updateSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.updateIndexSettings(updateSettingsRequest, options);
  }

  @Override
  @Nonnull
  public RefreshResponse refreshIndex(
      @Nonnull RefreshRequest refreshRequest, @Nonnull RequestOptions options) throws IOException {
    return delegate.refreshIndex(refreshRequest, options);
  }

  @Override
  @Nonnull
  public GetAliasesResponse getIndexAliases(
      @Nonnull GetAliasesRequest getAliasesRequest, @Nonnull RequestOptions options)
      throws IOException {
    return delegate.getIndexAliases(getAliasesRequest, options);
  }

  @Override
  @Nonnull
  public AcknowledgedResponse updateIndexAliases(
      IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException {
    return delegate.updateIndexAliases(indicesAliasesRequest, options);
  }

  @Override
  @Nonnull
  public AnalyzeResponse analyzeIndex(AnalyzeRequest request, RequestOptions options)
      throws IOException {
    return delegate.analyzeIndex(request, options);
  }

  @Override
  @Nonnull
  public ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException {
    return delegate.getClusterSettings(clusterGetSettingsRequest, options);
  }

  @Override
  @Nonnull
  public ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException {
    return delegate.putClusterSettings(clusterUpdateSettingsRequest, options);
  }

  @Override
  @Nonnull
  public ClusterHealthResponse clusterHealth(
      ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
    return delegate.clusterHealth(healthRequest, options);
  }

  @Override
  @Nonnull
  public ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options)
      throws IOException {
    return delegate.listTasks(request, options);
  }

  @Override
  @Nonnull
  public Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException {
    return delegate.getTask(request, options);
  }

  @Override
  @Nonnull
  public SearchClientShim.SearchEngineType getEngineType() {
    return delegate.getEngineType();
  }

  @Override
  @Nonnull
  public String getEngineVersion() throws IOException {
    return delegate.getEngineVersion();
  }

  @Override
  @Nonnull
  public Map<String, String> getClusterInfo() throws IOException {
    return delegate.getClusterInfo();
  }

  @Override
  public boolean supportsFeature(@Nonnull String feature) {
    return delegate.supportsFeature(feature);
  }

  @Override
  @Nonnull
  public RawResponse performLowLevelRequest(Request request) throws IOException {
    return delegate.performLowLevelRequest(request);
  }

  @Override
  @Nonnull
  public BulkByScrollResponse updateByQuery(
      UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException {
    return delegate.updateByQuery(updateByQueryRequest, options);
  }

  @Override
  @Nonnull
  public String submitDeleteByQueryTask(
      DeleteByQueryRequest deleteByQueryRequest, RequestOptions options) throws IOException {
    return delegate.submitDeleteByQueryTask(deleteByQueryRequest, options);
  }

  @Override
  @Nonnull
  public String submitReindexTask(ReindexRequest reindexRequest, RequestOptions options)
      throws IOException {
    return delegate.submitReindexTask(reindexRequest, options);
  }

  @Override
  public void generateAsyncBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount) {
    delegate.generateAsyncBulkProcessor(
        writeRequestRefreshPolicy,
        metricUtils,
        bulkRequestsLimit,
        bulkFlushPeriod,
        retryInterval,
        numRetries,
        threadCount);
  }

  @Override
  public void generateBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount) {
    delegate.generateBulkProcessor(
        writeRequestRefreshPolicy,
        metricUtils,
        bulkRequestsLimit,
        bulkFlushPeriod,
        retryInterval,
        numRetries,
        threadCount);
  }

  @Override
  public void addBulk(DocWriteRequest<?> writeRequest) {
    delegate.addBulk(writeRequest);
  }

  @Override
  public void addBulk(String urn, DocWriteRequest<?> writeRequest) {
    delegate.addBulk(urn, writeRequest);
  }

  @Override
  public void flushBulkProcessor() {
    delegate.flushBulkProcessor();
  }

  @Override
  public void closeBulkProcessor() {
    delegate.closeBulkProcessor();
  }

  @Override
  @Nonnull
  public Object getNativeClient() {
    return delegate.getNativeClient();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
