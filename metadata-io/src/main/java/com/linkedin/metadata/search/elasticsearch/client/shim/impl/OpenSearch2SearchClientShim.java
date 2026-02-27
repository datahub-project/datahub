package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.linkedin.metadata.search.elasticsearch.client.shim.OpenSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.IOReactorExceptionHandler;
import org.apache.http.ssl.SSLContexts;
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
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkResponse;
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
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.indices.AnalyzeResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.client.indices.ResizeResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/**
 * Implementation of SearchClientShim using the OpenSearch 2.x REST High Level Client. This
 * implementation supports: - OpenSearch 2.x clusters
 *
 * <p>OpenSearch 2.x maintains compatibility with the ES 7.x REST high-level client API, so this
 * implementation is very similar to Es7CompatibilitySearchClientShim.
 */
@Slf4j
public class OpenSearch2SearchClientShim extends AbstractBulkProcessorShim<BulkProcessor>
    implements OpenSearchClientShim<RestHighLevelClient> {

  @Getter private final ShimConfiguration shimConfiguration;
  private final RestHighLevelClient client;
  protected SearchEngineType engineType;

  public OpenSearch2SearchClientShim(@Nonnull ShimConfiguration config) throws IOException {
    this.shimConfiguration = config;
    this.engineType = SearchEngineType.OPENSEARCH_2;
    this.client = createClient();

    log.info("Created OpenSearch 2.x shim for engine type: {}", engineType);
  }

  public RestHighLevelClient createClient() {
    final RestClientBuilder builder = createBuilder();

    builder.setHttpClientConfigCallback(
        httpAsyncClientBuilder -> {
          if (shimConfiguration.isUseSSL()) {
            httpAsyncClientBuilder
                .setSSLContext(shimConfiguration.getSSLContext())
                .setSSLHostnameVerifier(new NoopHostnameVerifier());
          }
          try {
            httpAsyncClientBuilder.setConnectionManager(createConnectionManager());
          } catch (IOReactorException e) {
            throw new IllegalStateException(
                "Unable to start ElasticSearch client. Please verify connection configuration.");
          }
          httpAsyncClientBuilder.setDefaultIOReactorConfig(
              IOReactorConfig.custom()
                  .setIoThreadCount(shimConfiguration.getThreadCount())
                  .build());

          setCredentials(httpAsyncClientBuilder);

          return httpAsyncClientBuilder;
        });

    return new RestHighLevelClient(builder);
  }

  @Nonnull
  private RestClientBuilder createBuilder() {
    String scheme = shimConfiguration.isUseSSL() ? "https" : "http";
    final RestClientBuilder builder =
        RestClient.builder(
            new HttpHost(shimConfiguration.getHost(), shimConfiguration.getPort(), scheme));

    if (!StringUtils.isEmpty(shimConfiguration.getPathPrefix())) {
      builder.setPathPrefix(shimConfiguration.getPathPrefix());
    }

    builder.setRequestConfigCallback(
        requestConfigBuilder ->
            requestConfigBuilder.setConnectionRequestTimeout(
                shimConfiguration.getConnectionRequestTimeout()));

    return builder;
  }

  /**
   * Needed to override ExceptionHandler behavior for cases where IO error would have put client in
   * unrecoverable state We don't utilize system properties in the client builder, so setting
   * defaults pulled from {@link HttpAsyncClientBuilder#build()}.
   *
   * @return
   */
  private NHttpClientConnectionManager createConnectionManager() throws IOReactorException {
    SSLContext sslContext =
        shimConfiguration.getSSLContext() == null
            ? SSLContexts.createDefault()
            : shimConfiguration.getSSLContext();
    HostnameVerifier hostnameVerifier =
        new DefaultHostnameVerifier(PublicSuffixMatcherLoader.getDefault());
    SchemeIOSessionStrategy sslStrategy =
        new SSLIOSessionStrategy(sslContext, null, null, hostnameVerifier);

    log.info("Creating IOReactorConfig with threadCount: {}", shimConfiguration.getThreadCount());
    IOReactorConfig ioReactorConfig =
        IOReactorConfig.custom().setIoThreadCount(shimConfiguration.getThreadCount()).build();
    DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    IOReactorExceptionHandler ioReactorExceptionHandler =
        new IOReactorExceptionHandler() {
          @Override
          public boolean handle(IOException ex) {
            log.error("IO Exception caught during ElasticSearch connection.", ex);
            return true;
          }

          @Override
          public boolean handle(RuntimeException ex) {
            log.error("Runtime Exception caught during ElasticSearch connection.", ex);
            return true;
          }
        };
    ioReactor.setExceptionHandler(ioReactorExceptionHandler);

    PoolingNHttpClientConnectionManager connectionManager =
        new PoolingNHttpClientConnectionManager(
            ioReactor,
            RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", NoopIOSessionStrategy.INSTANCE)
                .register("https", sslStrategy)
                .build());

    // Set maxConnectionsPerRoute to match threadCount (minimum 2)
    int maxConnectionsPerRoute = Math.max(2, shimConfiguration.getThreadCount());
    connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);

    log.info(
        "Configured connection pool: maxPerRoute={} (threadCount={})",
        maxConnectionsPerRoute,
        shimConfiguration.getThreadCount());

    return connectionManager;
  }

  private void setCredentials(HttpAsyncClientBuilder httpAsyncClientBuilder) {
    if (shimConfiguration.getUsername() != null && shimConfiguration.getPassword() != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(
              shimConfiguration.getUsername(), shimConfiguration.getPassword()));
      httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }
    if (shimConfiguration.isUseAwsIamAuth()) {
      HttpRequestInterceptor interceptor =
          getAwsRequestSigningInterceptor(shimConfiguration.getRegion());
      httpAsyncClientBuilder.addInterceptorLast(interceptor);
    }
  }

  private HttpRequestInterceptor getAwsRequestSigningInterceptor(String region) {

    if (region == null) {
      throw new IllegalArgumentException(
          "Region must not be null when opensearchUseAwsIamAuth is enabled");
    }
    Aws4Signer signer = Aws4Signer.create();
    // Uses default AWS credentials
    return new AwsRequestSigningApacheInterceptor(
        "es", signer, DefaultCredentialsProvider.create(), region);
  }

  // Core search operations
  @Nonnull
  @Override
  public SearchResponse search(
      @Nonnull SearchRequest searchRequest, @Nonnull RequestOptions options) throws IOException {
    return client.search(searchRequest, options);
  }

  @Nonnull
  @Override
  public SearchResponse scroll(
      @Nonnull SearchScrollRequest searchScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.scroll(searchScrollRequest, options);
  }

  @Nonnull
  @Override
  public ClearScrollResponse clearScroll(
      @Nonnull ClearScrollRequest clearScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.clearScroll(clearScrollRequest, options);
  }

  @Nonnull
  @Override
  public CountResponse count(@Nonnull CountRequest countRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.count(countRequest, options);
  }

  @Nonnull
  @Override
  public ExplainResponse explain(
      @Nonnull ExplainRequest explainRequest, @Nonnull RequestOptions options) throws IOException {
    return client.explain(explainRequest, options);
  }

  // Document operations
  @Nonnull
  @Override
  public GetResponse getDocument(@Nonnull GetRequest getRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.get(getRequest, options);
  }

  @Nonnull
  @Override
  public IndexResponse indexDocument(
      @Nonnull IndexRequest indexRequest, @Nonnull RequestOptions options) throws IOException {
    return client.index(indexRequest, options);
  }

  @Nonnull
  @Override
  public DeleteResponse deleteDocument(
      @Nonnull DeleteRequest deleteRequest, @Nonnull RequestOptions options) throws IOException {
    return client.delete(deleteRequest, options);
  }

  @Nonnull
  @Override
  public BulkByScrollResponse deleteByQuery(
      @Nonnull DeleteByQueryRequest deleteByQueryRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.deleteByQuery(deleteByQueryRequest, options);
  }

  @Nonnull
  @Override
  public CreatePitResponse createPit(
      @Nonnull CreatePitRequest createPitRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.createPit(createPitRequest, options);
  }

  @Nonnull
  @Override
  public DeletePitResponse deletePit(
      @Nonnull DeletePitRequest deletePitRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.deletePit(deletePitRequest, options);
  }

  // Index management operations
  @Nonnull
  @Override
  public CreateIndexResponse createIndex(
      @Nonnull CreateIndexRequest createIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().create(createIndexRequest, options);
  }

  @Nonnull
  @Override
  public com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse getIndex(
      GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
    GetIndexResponse indexResponse = client.indices().get(getIndexRequest, options);
    return new com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse(
        indexResponse.getIndices(),
        indexResponse.getMappings(),
        indexResponse.getAliases(),
        indexResponse.getSettings(),
        indexResponse.getDefaultSettings(),
        indexResponse.getDataStreams());
  }

  @Nonnull
  @Override
  public ResizeResponse cloneIndex(ResizeRequest resizeRequest, RequestOptions options)
      throws IOException {
    return client.indices().clone(resizeRequest, options);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse deleteIndex(
      @Nonnull DeleteIndexRequest deleteIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().delete(deleteIndexRequest, options);
  }

  @Override
  public boolean indexExists(
      @Nonnull GetIndexRequest getIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().exists(getIndexRequest, options);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse putIndexMapping(
      @Nonnull PutMappingRequest putMappingRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().putMapping(putMappingRequest, options);
  }

  @Nonnull
  @Override
  public GetMappingsResponse getIndexMapping(
      @Nonnull GetMappingsRequest getMappingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().getMapping(getMappingsRequest, options);
  }

  @Nonnull
  @Override
  public GetSettingsResponse getIndexSettings(
      @Nonnull GetSettingsRequest getSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().getSettings(getSettingsRequest, options);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexSettings(
      @Nonnull UpdateSettingsRequest updateSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().putSettings(updateSettingsRequest, options);
  }

  @Nonnull
  @Override
  public RefreshResponse refreshIndex(
      @Nonnull RefreshRequest refreshRequest, @Nonnull RequestOptions options) throws IOException {
    return client.indices().refresh(refreshRequest, options);
  }

  @Nonnull
  @Override
  public GetAliasesResponse getIndexAliases(
      @Nonnull GetAliasesRequest getAliasesRequest, @Nonnull RequestOptions options)
      throws IOException {
    return client.indices().getAlias(getAliasesRequest, options);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexAliases(
      IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException {
    return client.indices().updateAliases(indicesAliasesRequest, options);
  }

  @Nonnull
  @Override
  public AnalyzeResponse analyzeIndex(AnalyzeRequest request, RequestOptions options)
      throws IOException {
    return client.indices().analyze(request, options);
  }

  @Nonnull
  @Override
  public ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException {
    return client.cluster().getSettings(clusterGetSettingsRequest, options);
  }

  @Nonnull
  @Override
  public ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException {
    return client.cluster().putSettings(clusterUpdateSettingsRequest, options);
  }

  @Nonnull
  @Override
  public ClusterHealthResponse clusterHealth(
      ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
    return client.cluster().health(healthRequest, options);
  }

  @Nonnull
  @Override
  public ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options)
      throws IOException {
    return client.tasks().list(request, options);
  }

  @Nonnull
  @Override
  public Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException {
    return client.tasks().get(request, options);
  }

  // Metadata and introspection
  @Nonnull
  @Override
  public SearchEngineType getEngineType() {
    return engineType;
  }

  @Nonnull
  @Override
  public String getEngineVersion() throws IOException {
    try {
      Map<String, String> clusterInfo = getClusterInfo();
      return clusterInfo.getOrDefault("version", "unknown");
    } catch (Exception e) {
      log.warn("Failed to get engine version", e);
      return "unknown";
    }
  }

  @Nonnull
  @Override
  public Map<String, String> getClusterInfo() throws IOException {
    try {
      // Use the info() API to get cluster information
      org.opensearch.client.core.MainResponse info = client.info(RequestOptions.DEFAULT);

      Map<String, String> clusterInfo = new HashMap<>();
      clusterInfo.put("cluster_name", info.getClusterName());
      clusterInfo.put("cluster_uuid", info.getClusterUuid());
      clusterInfo.put("version", info.getVersion().getNumber());
      clusterInfo.put("build_flavor", info.getVersion().getBuildType());
      clusterInfo.put("build_type", info.getVersion().getBuildType());
      clusterInfo.put("build_hash", info.getVersion().getBuildHash());
      clusterInfo.put("build_date", info.getVersion().getBuildDate());
      clusterInfo.put("lucene_version", info.getVersion().getLuceneVersion());
      clusterInfo.put("engine_type", "opensearch");

      return clusterInfo;
    } catch (Exception e) {
      log.error("Failed to get cluster info", e);
      throw new IOException("Failed to retrieve cluster information", e);
    }
  }

  @Override
  public boolean supportsFeature(@Nonnull String feature) {
    switch (feature) {
      case "scroll":
      case "bulk":
      case "mapping_types":
      case "point_in_time":
        return true;
      case "async_search":
        // OpenSearch has async search starting from 1.0
        return true;
      case "cross_cluster_replication":
        // CCR is not available in OpenSearch
        return false;
      default:
        log.warn("Unknown feature requested: {}", feature);
        return false;
    }
  }

  @Nonnull
  @Override
  public RawResponse performLowLevelRequest(Request request) throws IOException {
    Response response = client.getLowLevelClient().performRequest(request);
    return new RawResponse(
        response.getRequestLine(),
        response.getHost(),
        response.getEntity(),
        response.getStatusLine());
  }

  @Nonnull
  @Override
  public BulkByScrollResponse updateByQuery(
      UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException {
    return client.updateByQuery(updateByQueryRequest, options);
  }

  @Nonnull
  @Override
  public String submitDeleteByQueryTask(
      DeleteByQueryRequest deleteByQueryRequest, RequestOptions options) throws IOException {
    return client.submitDeleteByQueryTask(deleteByQueryRequest, options).getTask();
  }

  /**
   * Submits reindex asynchronously (OpenSearch REST client's submitReindexTask returns a task id;
   * wait_for_completion is handled by the client for this API). Aligned with Es8SearchClientShim
   * which sets waitForCompletion(false) for async reindex.
   */
  @Nonnull
  @Override
  public String submitReindexTask(ReindexRequest reindexRequest, RequestOptions options)
      throws IOException {
    return client.submitReindexTask(reindexRequest, options).getTask();
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
    Supplier<BulkProcessor> processorSupplier =
        () ->
            BulkProcessor.builder(
                    (request, bulkListener) -> {
                      client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
                    },
                    BulkListener.getInstance(0, writeRequestRefreshPolicy, metricUtils))
                .setBulkActions(bulkRequestsLimit)
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
                .setBackoffPolicy(
                    BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(retryInterval), numRetries))
                .build();

    initBulkProcessors(threadCount, processorSupplier);

    log.info("Initialized {} async bulk processors for parallel execution", threadCount);
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
    Supplier<BulkProcessor> processorSupplier =
        () ->
            BulkProcessor.builder(
                    (request, bulkListener) -> {
                      try {
                        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
                        bulkListener.onResponse(response);
                      } catch (IOException e) {
                        bulkListener.onFailure(e);
                        throw new RuntimeException(e);
                      }
                    },
                    BulkListener.getInstance(0, writeRequestRefreshPolicy, metricUtils))
                .setBulkActions(bulkRequestsLimit)
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
                .setBackoffPolicy(
                    BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(retryInterval), numRetries))
                .build();

    initBulkProcessors(threadCount, processorSupplier);

    log.info("Initialized {} bulk processors for parallel execution", threadCount);
  }

  @Override
  protected void addToProcessor(BulkProcessor processor, DocWriteRequest<?> writeRequest) {
    processor.add(writeRequest);
  }

  @Override
  protected void flushProcessor(BulkProcessor processor) {
    processor.flush();
  }

  @Override
  protected void closeProcessor(BulkProcessor processor) {
    processor.close();
  }

  @Nonnull
  @Override
  public RestHighLevelClient getNativeClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }
}
