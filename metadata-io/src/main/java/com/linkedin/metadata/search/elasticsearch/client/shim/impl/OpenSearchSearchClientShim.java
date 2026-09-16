package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.client.shim.OpenSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2KnnQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2SemanticIndexMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2.OpenSearch2SemanticIndexSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.EmbeddingBatch;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
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
import org.apache.http.util.EntityUtils;
import org.opensearch.OpenSearchStatusException;
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
import org.opensearch.action.bulk.BulkRequest;
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
import org.opensearch.client.OpenSearchShimBridge;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.core.MainResponse;
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
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.BytesRestResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/**
 * Unified OpenSearch shim serving OpenSearch 2.x and 3.x through the (non-deprecated) low-level
 * {@link RestClient}.
 *
 * <p>Wire behavior is identical to the legacy REST high-level client by construction: requests are
 * produced by the RHLC's own request converters (via {@link OpenSearchShimBridge}) and responses
 * are parsed with the same public {@code fromXContent} parsers and named-XContent registry the RHLC
 * uses internally. The {@code RestHighLevelClient} itself performs no I/O for OS2/OS3; it remains
 * on the classpath as a type library and as the ES 7.x transport ({@link
 * Es7CompatibilitySearchClientShim}).
 */
@Slf4j
public class OpenSearchSearchClientShim extends AbstractBulkProcessorShim<BulkProcessor>
    implements OpenSearchClientShim<RestClient> {

  private static final Pattern SEMANTIC_VERSION_PATTERN =
      Pattern.compile(
          "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
              + "(?:-((?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)"
              + "(?:\\.(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*))*))?"
              + "(?:\\+([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?$");

  /**
   * OpenSearch persists {@code doc_values: false} on {@code search_as_you_type} fields, so the
   * authored mapping must include it to round-trip cleanly against {@code GetMapping}. Same
   * behavior on 2.x and 3.x.
   */
  public static final Map<String, String> PARTIAL_NGRAM_CONFIG =
      ImmutableMap.of(
          "type", "search_as_you_type",
          "max_shingle_size", "4",
          "doc_values", "false");

  @Getter private final ShimConfiguration shimConfiguration;
  private final RestClient restClient;
  private final ObjectMapper objectMapper;
  private final NamedXContentRegistry xContentRegistry;
  protected SearchEngineType engineType;

  public OpenSearchSearchClientShim(@Nonnull ShimConfiguration config) throws IOException {
    // Reject rather than coerce: silently treating an Elasticsearch engine type as OPENSEARCH_2
    // would mask a mis-wired factory (this shim only speaks the OpenSearch wire protocol).
    if (config.getEngineType() == null || !config.getEngineType().isOpenSearch()) {
      throw new IllegalArgumentException(
          "OpenSearchSearchClientShim requires an OpenSearch engine type, got: "
              + config.getEngineType());
    }
    this.shimConfiguration = config;
    this.engineType = config.getEngineType();
    this.restClient = createLowLevelClient();
    this.objectMapper = new ObjectMapper();
    this.xContentRegistry = OpenSearchShimBridge.defaultRegistry();

    log.info("Created unified OpenSearch shim for engine type: {}", engineType);
  }

  /** Package-private factory for tests; avoids spinning up a real OS connection. */
  static OpenSearchSearchClientShim forTest(RestClient restClient) {
    return new OpenSearchSearchClientShim(restClient, new ObjectMapper(), null);
  }

  /** Package-private factory for tests that need a specific configuration. */
  static OpenSearchSearchClientShim forTest(RestClient restClient, ShimConfiguration config) {
    return new OpenSearchSearchClientShim(restClient, new ObjectMapper(), config);
  }

  private OpenSearchSearchClientShim(
      RestClient restClient, ObjectMapper objectMapper, ShimConfiguration config) {
    this.shimConfiguration = config;
    this.engineType =
        config != null && config.getEngineType() == SearchEngineType.OPENSEARCH_3
            ? SearchEngineType.OPENSEARCH_3
            : SearchEngineType.OPENSEARCH_2;
    this.restClient = restClient;
    this.objectMapper = objectMapper;
    this.xContentRegistry = OpenSearchShimBridge.defaultRegistry();
  }

  /** The version probe is only benign when the engine type did not have to be auto-detected. */
  private boolean isEngineTypeAutoDetected() {
    return shimConfiguration != null && shimConfiguration.isEngineTypeAutoDetected();
  }

  // Client construction (same connection mechanics as the legacy RHLC-based shim)

  private RestClient createLowLevelClient() {
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
                "Unable to start OpenSearch client. Please verify connection configuration.");
          }
          httpAsyncClientBuilder.setDefaultIOReactorConfig(
              IOReactorConfig.custom()
                  .setIoThreadCount(shimConfiguration.getThreadCount())
                  .setSoTimeout(shimConfiguration.getSocketTimeout())
                  .build());

          setCredentials(httpAsyncClientBuilder);

          return httpAsyncClientBuilder;
        });

    return builder.build();
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
            requestConfigBuilder
                .setConnectionRequestTimeout(shimConfiguration.getConnectionRequestTimeout())
                .setSocketTimeout(shimConfiguration.getSocketTimeout()));

    return builder;
  }

  /**
   * Needed to override ExceptionHandler behavior for cases where IO error would have put client in
   * unrecoverable state. We don't utilize system properties in the client builder, so setting
   * defaults pulled from {@link HttpAsyncClientBuilder#build()}.
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

    log.info(
        "Creating IOReactorConfig with threadCount: {}, socketTimeout: {}ms",
        shimConfiguration.getThreadCount(),
        shimConfiguration.getSocketTimeout());
    IOReactorConfig ioReactorConfig =
        IOReactorConfig.custom()
            .setIoThreadCount(shimConfiguration.getThreadCount())
            .setSoTimeout(shimConfiguration.getSocketTimeout())
            .build();
    DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    IOReactorExceptionHandler ioReactorExceptionHandler =
        new IOReactorExceptionHandler() {
          @Override
          public boolean handle(IOException ex) {
            log.error("IO Exception caught during OpenSearch connection.", ex);
            return true;
          }

          @Override
          public boolean handle(RuntimeException ex) {
            log.error("Runtime Exception caught during OpenSearch connection.", ex);
            return true;
          }
        };
    ioReactor.setExceptionHandler(ioReactorExceptionHandler);

    PoolingNHttpClientConnectionManager connectionManager =
        new PoolingNHttpClientConnectionManager(
            ioReactor,
            org.apache.http.config.RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", NoopIOSessionStrategy.INSTANCE)
                .register("https", sslStrategy)
                .build());

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
    // Reuse the shared, GMS-managed credential provider (INC-5436): creating one per client leaks
    // IRSA refresh tasks in the AWS SDK's sdk-cache-scheduler.
    AwsCredentialsProvider credentialsProvider = shimConfiguration.getAwsCredentialsProvider();
    if (credentialsProvider == null) {
      throw new IllegalStateException(
          "AwsCredentialsProvider must be configured when opensearchUseAwsIamAuth is enabled");
    }
    Aws4Signer signer = Aws4Signer.create();
    return new AwsRequestSigningApacheInterceptor("es", signer, credentialsProvider, region);
  }

  // Request execution + response parsing (RHLC mechanics without RestHighLevelClient)

  private Response perform(@Nonnull Request request, @Nonnull RequestOptions options)
      throws IOException {
    request.setOptions(options);
    try {
      return restClient.performRequest(request);
    } catch (ResponseException e) {
      throw translateException(e);
    }
  }

  /**
   * Executes and parses, treating {@code allowedErrorStatus} as a parseable body rather than an
   * error — mirroring the RHLC's per-API "ignores" sets (e.g. GET-document 404s carry a {@code
   * found: false} body).
   */
  private <R> R performAndParse(
      @Nonnull Request request,
      @Nonnull RequestOptions options,
      @Nonnull CheckedFunction<XContentParser, R, IOException> entityParser,
      int... allowedErrorStatus)
      throws IOException {
    request.setOptions(options);
    Response response;
    try {
      response = restClient.performRequest(request);
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      for (int allowed : allowedErrorStatus) {
        if (status == allowed) {
          return parseEntity(e.getResponse().getEntity(), entityParser);
        }
      }
      throw translateException(e);
    }
    return parseEntity(response.getEntity(), entityParser);
  }

  private <R> R parseEntity(
      HttpEntity entity, @Nonnull CheckedFunction<XContentParser, R, IOException> entityParser)
      throws IOException {
    if (entity == null) {
      throw new IllegalStateException("Response body expected but not returned");
    }
    if (entity.getContentType() == null) {
      throw new IllegalStateException("OpenSearch didn't return the [Content-Type] header");
    }
    MediaType medaType = MediaTypeRegistry.fromMediaType(entity.getContentType().getValue());
    if (medaType == null) {
      throw new IllegalStateException(
          "Unsupported Content-Type: " + entity.getContentType().getValue());
    }
    try (XContentParser parser =
        medaType
            .xContent()
            .createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE, entity.getContent())) {
      return entityParser.apply(parser);
    }
  }

  /**
   * Converts a low-level {@link ResponseException} into the {@link OpenSearchStatusException} the
   * high-level client would have thrown, parsing the structured error body when present.
   */
  private OpenSearchStatusException translateException(ResponseException e) {
    RestStatus status = RestStatus.fromCode(e.getResponse().getStatusLine().getStatusCode());
    try {
      OpenSearchStatusException parsed =
          parseEntity(e.getResponse().getEntity(), BytesRestResponse::errorFromXContent);
      // Re-wrap to attach the transport-layer exception as cause, matching RHLC behavior.
      OpenSearchStatusException result =
          new OpenSearchStatusException(parsed.getMessage(), status, parsed);
      result.addSuppressed(e);
      return result;
    } catch (Exception parseFailure) {
      OpenSearchStatusException result = new OpenSearchStatusException(e.getMessage(), status, e);
      result.addSuppressed(parseFailure);
      return result;
    }
  }

  // Core search operations
  //
  // Raw impls ignore opContext — they are pure pass-throughs over the low-level client.
  // Per-event decoration (tenant routing, query filtering, etc.) belongs at the wrapper layer.
  @Nonnull
  @Override
  public SearchResponse search(
      @Nonnull OperationFingerprint opContext,
      @Nonnull SearchRequest searchRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.search(searchRequest), options, SearchResponse::fromXContent);
  }

  @Nonnull
  @Override
  public SearchResponse scroll(
      @Nonnull OperationFingerprint opContext,
      @Nonnull SearchScrollRequest searchScrollRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.scroll(searchScrollRequest), options, SearchResponse::fromXContent);
  }

  @Nonnull
  @Override
  public ClearScrollResponse clearScroll(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ClearScrollRequest clearScrollRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.clearScroll(clearScrollRequest),
        options,
        ClearScrollResponse::fromXContent);
  }

  @Nonnull
  @Override
  public CountResponse count(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CountRequest countRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.count(countRequest), options, CountResponse::fromXContent);
  }

  @Nonnull
  @Override
  public ExplainResponse explain(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ExplainRequest explainRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    // RHLC allows 404 here: the body still carries the explain payload with matched=false /
    // missing-doc info; exists mirrors the HTTP status.
    Request request = OpenSearchShimBridge.explain(explainRequest);
    request.setOptions(options);
    Response response;
    boolean exists;
    HttpEntity entity;
    try {
      response = restClient.performRequest(request);
      exists = true;
      entity = response.getEntity();
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
        exists = false;
        entity = e.getResponse().getEntity();
      } else {
        throw translateException(e);
      }
    }
    final boolean finalExists = exists;
    return parseEntity(entity, parser -> ExplainResponse.fromXContent(parser, finalExists));
  }

  // Document operations
  @Nonnull
  @Override
  public GetResponse getDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetRequest getRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    // 404 on an existing index returns a parseable found=false body, matching RHLC's allowed set.
    return performAndParse(
        OpenSearchShimBridge.get(getRequest),
        options,
        GetResponse::fromXContent,
        RestStatus.NOT_FOUND.getStatus());
  }

  @Nonnull
  @Override
  public IndexResponse indexDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull IndexRequest indexRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.index(indexRequest), options, IndexResponse::fromXContent);
  }

  @Nonnull
  @Override
  public DeleteResponse deleteDocument(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteRequest deleteRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.delete(deleteRequest),
        options,
        DeleteResponse::fromXContent,
        RestStatus.NOT_FOUND.getStatus());
  }

  @Nonnull
  @Override
  public BulkByScrollResponse deleteByQuery(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteByQueryRequest deleteByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.deleteByQuery(deleteByQueryRequest),
        options,
        BulkByScrollResponse::fromXContent);
  }

  @Nonnull
  @Override
  public CreatePitResponse createPit(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CreatePitRequest createPitRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.createPit(createPitRequest), options, CreatePitResponse::fromXContent);
  }

  @Nonnull
  @Override
  public DeletePitResponse deletePit(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeletePitRequest deletePitRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.deletePit(deletePitRequest), options, DeletePitResponse::fromXContent);
  }

  // Index management operations
  @Nonnull
  @Override
  public CreateIndexResponse createIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull CreateIndexRequest createIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.createIndex(createIndexRequest),
        options,
        CreateIndexResponse::fromXContent);
  }

  @Nonnull
  @Override
  public com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse getIndex(
      @Nonnull OperationFingerprint opContext,
      GetIndexRequest getIndexRequest,
      RequestOptions options)
      throws IOException {
    GetIndexResponse indexResponse =
        performAndParse(
            OpenSearchShimBridge.getIndex(getIndexRequest),
            options,
            GetIndexResponse::fromXContent);
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
  public ResizeResponse cloneIndex(
      @Nonnull OperationFingerprint opContext, ResizeRequest resizeRequest, RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.cloneIndex(resizeRequest), options, ResizeResponse::fromXContent);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse deleteIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteIndexRequest deleteIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.deleteIndex(deleteIndexRequest),
        options,
        AcknowledgedResponse::fromXContent);
  }

  @Override
  public boolean indexExists(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetIndexRequest getIndexRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    Request request = OpenSearchShimBridge.indicesExist(getIndexRequest);
    request.setOptions(options);
    try {
      Response response = restClient.performRequest(request);
      return response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
        return false;
      }
      throw translateException(e);
    }
  }

  @Nonnull
  @Override
  public AcknowledgedResponse putIndexMapping(
      @Nonnull OperationFingerprint opContext,
      @Nonnull PutMappingRequest putMappingRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.putMapping(putMappingRequest),
        options,
        AcknowledgedResponse::fromXContent);
  }

  @Nonnull
  @Override
  public GetMappingsResponse getIndexMapping(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetMappingsRequest getMappingsRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.getMappings(getMappingsRequest),
        options,
        GetMappingsResponse::fromXContent);
  }

  @Nonnull
  @Override
  public GetSettingsResponse getIndexSettings(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetSettingsRequest getSettingsRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.getSettings(getSettingsRequest),
        options,
        GetSettingsResponse::fromXContent);
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexSettings(
      @Nonnull OperationFingerprint opContext,
      @Nonnull UpdateSettingsRequest updateSettingsRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.indexPutSettings(updateSettingsRequest),
        options,
        AcknowledgedResponse::fromXContent);
  }

  @Nonnull
  @Override
  public RefreshResponse refreshIndex(
      @Nonnull OperationFingerprint opContext,
      @Nonnull RefreshRequest refreshRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.refresh(refreshRequest), options, RefreshResponse::fromXContent);
  }

  @Nonnull
  @Override
  public GetAliasesResponse getIndexAliases(
      @Nonnull OperationFingerprint opContext,
      @Nonnull GetAliasesRequest getAliasesRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    // RHLC allows 404: the body still parses into a GetAliasesResponse carrying the error.
    return performAndParse(
        OpenSearchShimBridge.getAlias(getAliasesRequest),
        options,
        GetAliasesResponse::fromXContent,
        RestStatus.NOT_FOUND.getStatus());
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexAliases(
      @Nonnull OperationFingerprint opContext,
      IndicesAliasesRequest indicesAliasesRequest,
      RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.updateAliases(indicesAliasesRequest),
        options,
        AcknowledgedResponse::fromXContent);
  }

  @Nonnull
  @Override
  public AnalyzeResponse analyzeIndex(
      @Nonnull OperationFingerprint opContext, AnalyzeRequest request, RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.analyze(request), options, AnalyzeResponse::fromXContent);
  }

  // Cluster operations
  @Nonnull
  @Override
  public ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.clusterGetSettings(clusterGetSettingsRequest),
        options,
        ClusterGetSettingsResponse::fromXContent);
  }

  @Nonnull
  @Override
  public ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.clusterPutSettings(clusterUpdateSettingsRequest),
        options,
        ClusterUpdateSettingsResponse::fromXContent);
  }

  @Nonnull
  @Override
  public ClusterHealthResponse clusterHealth(
      ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
    // A wait_for_* timeout answers 408 with a full health body; parse it like RHLC does.
    return performAndParse(
        OpenSearchShimBridge.clusterHealth(healthRequest),
        options,
        ClusterHealthResponse::fromXContent,
        RestStatus.REQUEST_TIMEOUT.getStatus());
  }

  // Task operations
  @Nonnull
  @Override
  public ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.listTasks(request), options, ListTasksResponse::fromXContent);
  }

  @Nonnull
  @Override
  public Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException {
    Request lowLevelRequest = OpenSearchShimBridge.getTask(request);
    lowLevelRequest.setOptions(options);
    try {
      Response response = restClient.performRequest(lowLevelRequest);
      return Optional.of(parseEntity(response.getEntity(), GetTaskResponse::fromXContent));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
        return Optional.empty();
      }
      throw translateException(e);
    }
  }

  // Metadata and introspection
  @Nonnull
  @Override
  public SearchEngineType getEngineType() {
    return engineType;
  }

  @Nonnull
  @Override
  public Map<String, String> partialNgramConfig() {
    return PARTIAL_NGRAM_CONFIG;
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

  public void verifySemanticSearchSupport() throws IOException {
    verifySemanticSearchSupport(false);
  }

  public void verifySemanticSearchSupport(final boolean faissCosineConfigured) throws IOException {
    final String version = getEngineVersion();
    if ("unknown".equals(version)) {
      // Restricted users (e.g. AWS OpenSearch fine-grained access control without
      // cluster:monitor/main) cannot read the cluster info API. An unreadable version is
      // indeterminate, not proof of an unsupported cluster — warn and proceed rather than
      // failing startup.
      log.warn(
          "Unable to determine OpenSearch version (cluster info API unavailable or restricted); "
              + "skipping semantic search version verification. OpenSearch {} or newer is "
              + "required for the configured semantic/hybrid vector search.",
          faissCosineConfigured ? "2.19" : "2.17");
      return;
    }
    assertSemanticSearchSupported(version, faissCosineConfigured);
  }

  public static void assertSemanticSearchSupported(String version) {
    assertSemanticSearchSupported(version, false);
  }

  public static void assertSemanticSearchSupported(
      String version, final boolean faissCosineConfigured) {
    // Faiss added cosine similarity support in OpenSearch 2.19; other supported combinations
    // (e.g. Lucene cosine) work from 2.17. All OpenSearch 3.x versions qualify.
    final int minimumMinor = faissCosineConfigured ? 19 : 17;
    final String requirement =
        faissCosineConfigured
            ? "OpenSearch 2.19 or newer for faiss cosine similarity (or switch the model's"
                + " knnEngine/spaceType, e.g. to the lucene engine)"
            : "OpenSearch 2.17 or newer";
    final Matcher matcher = version != null ? SEMANTIC_VERSION_PATTERN.matcher(version) : null;
    if (matcher == null || !matcher.matches()) {
      throw new IllegalStateException(
          "Unable to verify OpenSearch version '"
              + version
              + "'. Semantic/hybrid vector search requires "
              + requirement
              + ".");
    }
    final int major = Integer.parseInt(matcher.group(1));
    final int minor = Integer.parseInt(matcher.group(2));
    final int patch = Integer.parseInt(matcher.group(3));
    final boolean belowMinimum = major < 2 || major == 2 && minor < minimumMinor;
    final boolean prereleaseOfMinimum =
        major == 2 && minor == minimumMinor && patch == 0 && matcher.group(4) != null;
    if (belowMinimum || prereleaseOfMinimum) {
      throw new IllegalStateException(
          "OpenSearch "
              + version
              + " is unsupported for the configured semantic/hybrid vector search; requires "
              + requirement
              + ".");
    }
  }

  @Nonnull
  @Override
  public Map<String, String> getClusterInfo() throws IOException {
    try {
      MainResponse info =
          performAndParse(
              OpenSearchShimBridge.info(), RequestOptions.DEFAULT, MainResponse::fromXContent);

      Map<String, String> clusterInfo = new HashMap<>();
      clusterInfo.put("cluster_name", info.getClusterName());
      clusterInfo.put("cluster_uuid", info.getClusterUuid());
      clusterInfo.put("version", info.getVersion().getNumber());
      clusterInfo.put("build_flavor", info.getVersion().getBuildType());
      clusterInfo.put("build_type", info.getVersion().getBuildType());
      clusterInfo.put("build_hash", info.getVersion().getBuildHash());
      clusterInfo.put("build_date", info.getVersion().getBuildDate());
      clusterInfo.put("engine_type", "opensearch");
      return clusterInfo;
    } catch (OpenSearchStatusException e) {
      if (e.status() == RestStatus.FORBIDDEN && !isEngineTypeAutoDetected()) {
        // Restricted roles (e.g. AWS OpenSearch fine-grained access control without
        // cluster:monitor/main) cannot call GET /. With an explicitly configured engine type,
        // callers treat an unreadable version as indeterminate and proceed, so this is an
        // expected condition in locked-down clusters, not an error. Under auto-detection the
        // version is required to pick the engine type, so that case stays at ERROR.
        log.warn(
            "Cluster info API is restricted for this user (missing cluster:monitor/main?): {}",
            e.getMessage());
      } else {
        log.error("Failed to get cluster info", e);
      }
      throw new IOException("Failed to retrieve cluster information", e);
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
  public RawResponse performLowLevelRequest(
      @Nonnull OperationFingerprint opContext, Request request) throws IOException {
    Response response = restClient.performRequest(request);
    return new RawResponse(
        response.getRequestLine(),
        response.getHost(),
        response.getEntity(),
        response.getStatusLine());
  }

  @Nonnull
  @Override
  public BulkByScrollResponse updateByQuery(
      @Nonnull OperationFingerprint opContext,
      @Nonnull UpdateByQueryRequest updateByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
        OpenSearchShimBridge.updateByQuery(updateByQueryRequest),
        options,
        BulkByScrollResponse::fromXContent);
  }

  @Nonnull
  @Override
  public String submitDeleteByQueryTask(
      @Nonnull OperationFingerprint opContext,
      @Nonnull DeleteByQueryRequest deleteByQueryRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
            OpenSearchShimBridge.submitDeleteByQuery(deleteByQueryRequest),
            options,
            TaskSubmissionResponse::fromXContent)
        .getTask();
  }

  /**
   * Submits reindex asynchronously (the submit converter sets wait_for_completion=false and returns
   * a task id). Aligned with Es8SearchClientShim which sets waitForCompletion(false) for async
   * reindex.
   */
  @Nonnull
  @Override
  public String submitReindexTask(
      @Nonnull OperationFingerprint opContext,
      @Nonnull ReindexRequest reindexRequest,
      @Nonnull RequestOptions options)
      throws IOException {
    return performAndParse(
            OpenSearchShimBridge.submitReindex(reindexRequest),
            options,
            TaskSubmissionResponse::fromXContent)
        .getTask();
  }

  // Bulk processors

  private void executeBulkSync(
      BulkRequest request, org.opensearch.core.action.ActionListener<BulkResponse> listener) {
    try {
      BulkResponse response =
          performAndParse(
              OpenSearchShimBridge.bulk(request),
              RequestOptions.DEFAULT,
              BulkResponse::fromXContent);
      listener.onResponse(response);
    } catch (Exception e) {
      listener.onFailure(e);
      throw new RuntimeException(e);
    }
  }

  private void executeBulkAsync(
      BulkRequest request, org.opensearch.core.action.ActionListener<BulkResponse> listener) {
    Request lowLevelRequest;
    try {
      lowLevelRequest = OpenSearchShimBridge.bulk(request);
    } catch (IOException e) {
      listener.onFailure(e);
      return;
    }
    lowLevelRequest.setOptions(RequestOptions.DEFAULT);
    restClient.performRequestAsync(
        lowLevelRequest,
        new org.opensearch.client.ResponseListener() {
          @Override
          public void onSuccess(Response response) {
            try {
              listener.onResponse(parseEntity(response.getEntity(), BulkResponse::fromXContent));
            } catch (Exception e) {
              listener.onFailure(e);
            }
          }

          @Override
          public void onFailure(Exception exception) {
            if (exception instanceof ResponseException) {
              listener.onFailure(translateException((ResponseException) exception));
            } else {
              listener.onFailure(exception);
            }
          }
        });
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
    final BulkListener[] listenerHolder = new BulkListener[1];
    Supplier<BulkProcessor> processorSupplier =
        () ->
            BulkProcessor.builder(this::executeBulkAsync, listenerHolder[0])
                .setBulkActions(bulkRequestsLimit)
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
                .setBackoffPolicy(
                    BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(retryInterval), numRetries))
                .build();

    initBulkProcessors(
        threadCount,
        processorSupplier,
        () ->
            listenerHolder[0] =
                BulkListener.create(
                    writeRequestRefreshPolicy,
                    metricUtils,
                    bulkWriteResultTracker,
                    bulkItemRequeueSupport));

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
    final BulkListener[] listenerHolder = new BulkListener[1];
    Supplier<BulkProcessor> processorSupplier =
        () ->
            BulkProcessor.builder(this::executeBulkSync, listenerHolder[0])
                .setBulkActions(bulkRequestsLimit)
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
                .setBackoffPolicy(
                    BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(retryInterval), numRetries))
                .build();

    initBulkProcessors(
        threadCount,
        processorSupplier,
        () ->
            listenerHolder[0] =
                BulkListener.create(
                    writeRequestRefreshPolicy,
                    metricUtils,
                    bulkWriteResultTracker,
                    bulkItemRequeueSupport));

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

  // Semantic / kNN operations

  @Nonnull
  @Override
  public KnnSearchResponse searchKnn(
      @Nonnull OperationFingerprint opContext, @Nonnull KnnSearchRequest request)
      throws IOException {
    Map<String, Object> body = OpenSearch2KnnQueryBuilder.build(request);
    String requestBody = objectMapper.writeValueAsString(body);

    String endpoint = "/" + request.indexName() + "/_search";
    Request lowLevelReq = new Request("POST", endpoint);
    lowLevelReq.setJsonEntity(requestBody);
    lowLevelReq.addParameter("ignore_unavailable", String.valueOf(request.ignoreUnavailable()));
    // Always allow zero-index resolution, matching the ES8 shim; semantic search on partial
    // rollouts may target indices that do not yet exist on every node.
    lowLevelReq.addParameter("allow_no_indices", "true");

    Response response = restClient.performRequest(lowLevelReq);
    String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
    JsonNode responseJson = objectMapper.readTree(responseBody);

    return parseSearchKnnResponse(responseJson, objectMapper);
  }

  /**
   * Parses a kNN search response JSON node into a {@link KnnSearchResponse}.
   *
   * <p>Package-private for unit testing without a live cluster. The {@code mapper} is supplied by
   * the caller so production code reuses the shim's configured {@link ObjectMapper} and tests can
   * pass their own.
   */
  static KnnSearchResponse parseSearchKnnResponse(JsonNode responseJson, ObjectMapper mapper) {
    List<KnnSearchResponse.Hit> hits = new ArrayList<>();
    for (JsonNode hit : responseJson.path("hits").path("hits")) {
      String id = hit.path("_id").asText("");
      if (id.isEmpty()) {
        log.warn("OpenSearch kNN hit missing _id; skipping");
        continue;
      }
      double score;
      if (hit.path("_score").isNull() || hit.path("_score").isMissingNode()) {
        log.warn("OpenSearch kNN hit {} missing _score; defaulting to 0.0", id);
        score = 0.0;
      } else {
        score = hit.path("_score").asDouble(0.0);
      }
      Map<String, Object> source =
          hit.has("_source")
              ? mapper.convertValue(
                  hit.path("_source"), new TypeReference<Map<String, Object>>() {})
              : Map.of();
      hits.add(new KnnSearchResponse.Hit(id, score, source));
    }
    return new KnnSearchResponse(hits);
  }

  /** Recursively search for a "text" string field in a JSON node. */
  private static String findTextField(JsonNode node) {
    if (node == null || node.isMissingNode() || !node.isObject()) {
      return null;
    }
    JsonNode textNode = node.get("text");
    if (textNode != null && textNode.isTextual()) {
      return textNode.asText();
    }
    Iterator<JsonNode> children = node.elements();
    while (children.hasNext()) {
      JsonNode child = children.next();
      if (child.isObject()) {
        String found = findTextField(child);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  @Override
  public void createSemanticIndex(@Nonnull SemanticIndexSpec spec) throws IOException {
    Map<String, Object> mapping = OpenSearch2SemanticIndexMapper.build(spec);
    Map<String, Object> settings = OpenSearch2SemanticIndexSettingsBuilder.build(spec);

    CreateIndexRequest req = new CreateIndexRequest(spec.indexName());
    req.mapping(mapping);
    req.settings(settings);

    CreateIndexResponse resp =
        performAndParse(
            OpenSearchShimBridge.createIndex(req),
            RequestOptions.DEFAULT,
            CreateIndexResponse::fromXContent);
    if (!resp.isAcknowledged()) {
      throw new IOException("Index create not acknowledged: " + spec.indexName());
    }
    log.info("Created OpenSearch semantic index {}", spec.indexName());
  }

  @Override
  public void indexEmbeddings(
      @Nonnull OperationFingerprint opContext, @Nonnull EmbeddingBatch batch) throws IOException {
    Map<String, Object> document = buildEmbeddingsDocument(batch);

    IndexRequest req = new IndexRequest(batch.indexName());
    req.id(batch.documentId());
    req.source(document);

    IndexResponse resp =
        performAndParse(
            OpenSearchShimBridge.index(req), RequestOptions.DEFAULT, IndexResponse::fromXContent);
    if (resp.getResult() != org.opensearch.action.DocWriteResponse.Result.CREATED
        && resp.getResult() != org.opensearch.action.DocWriteResponse.Result.UPDATED) {
      throw new IOException(
          "Embedding index for " + batch.documentId() + " returned " + resp.getResult());
    }
    log.debug(
        "Indexed {} chunks for {} in {}",
        batch.chunks().size(),
        batch.documentId(),
        batch.indexName());
  }

  private static Map<String, Object> buildEmbeddingsDocument(EmbeddingBatch batch) {
    List<Map<String, Object>> chunks = new ArrayList<>(batch.chunks().size());
    for (EmbeddingBatch.Chunk c : batch.chunks()) {
      Map<String, Object> chunk = new LinkedHashMap<>();
      chunk.put("vector", c.vector());
      chunk.put("text", c.text());
      chunk.put("position", c.position());
      chunk.put("characterOffset", c.characterOffset());
      chunk.put("characterLength", c.characterLength());
      chunk.put("tokenCount", c.tokenCount());
      chunks.add(chunk);
    }
    Map<String, Object> modelEntry = Map.of("chunks", chunks);
    Map<String, Object> embeddings = Map.of(batch.modelKey(), modelEntry);
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("urn", batch.documentId());
    document.put("embeddings", embeddings);
    return document;
  }

  @Nonnull
  @Override
  public RestClient getNativeClient() {
    return restClient;
  }

  @Override
  public void close() throws IOException {
    if (restClient != null) {
      restClient.close();
    }
  }
}
