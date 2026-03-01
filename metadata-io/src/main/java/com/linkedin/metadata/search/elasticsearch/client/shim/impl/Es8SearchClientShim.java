package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Retries;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.SlicedScroll;
import co.elastic.clients.elasticsearch._types.Slices;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsRequest;
import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.ReindexResponse;
import co.elastic.clients.elasticsearch.core.UpdateByQueryResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.elasticsearch.core.reindex.Destination;
import co.elastic.clients.elasticsearch.core.reindex.Source;
import co.elastic.clients.elasticsearch.core.search.FieldSuggester;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.HighlighterEncoder;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import co.elastic.clients.elasticsearch.core.search.Rescore;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.Suggester;
import co.elastic.clients.elasticsearch.core.search.TrackHits;
import co.elastic.clients.elasticsearch.indices.CloneIndexRequest;
import co.elastic.clients.elasticsearch.indices.CloneIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.tasks.GetTasksRequest;
import co.elastic.clients.elasticsearch.tasks.GetTasksResponse;
import co.elastic.clients.elasticsearch.tasks.ListRequest;
import co.elastic.clients.elasticsearch.tasks.ListResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpUtils;
import co.elastic.clients.json.LazyDeserializer;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.DefaultTransportOptions;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.http.HeaderMap;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.ElasticSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8.CustomQuery;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8.Es8BulkListener;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.OriginalIndices;
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
import org.opensearch.action.bulk.BulkItemResponse;
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
import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
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
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.search.SearchException;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.SuggestionBuilder;

/**
 * Implementation of SearchClientShim using the Elasticsearch 8.x/9.x Java Client. This
 * implementation supports: - Elasticsearch 8.x clusters - Elasticsearch 9.x clusters Note: despite
 * requests allowing request options to be passed in per request, the new ES client does not support
 * this, since we always use the default this is likely not a concern, but something to keep in mind
 */
@Slf4j
public class Es8SearchClientShim extends AbstractBulkProcessorShim<BulkIngester<?>>
    implements ElasticSearchClientShim<ElasticsearchClient> {

  @Getter private final ShimConfiguration shimConfiguration;
  private final SearchEngineType engineType;
  private final ElasticsearchClient client;
  private final ObjectMapper objectMapper;
  private final JacksonJsonpMapper jacksonJsonpMapper;

  static {
    try {
      Field deserializerField = LazyDeserializer.class.getDeclaredField("deserializer");
      deserializerField.setAccessible(true);
      LazyDeserializer<Query> lazyDeserializer = (LazyDeserializer<Query>) Query._DESERIALIZER;
      deserializerField.set(lazyDeserializer, CustomQuery._DESERIALIZER);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public Es8SearchClientShim(@Nonnull ShimConfiguration config, ObjectMapper objectMapper)
      throws IOException {
    this.shimConfiguration = config;
    this.engineType = config.getEngineType();
    this.client = createEs8Client(config);
    this.objectMapper = objectMapper;
    this.jacksonJsonpMapper = new JacksonJsonpMapper(objectMapper);

    log.info("Created ElasticSearch 8.x shim for engine type: {}", engineType);
  }

  private ElasticsearchClient createEs8Client(ShimConfiguration config) throws IOException {
    RestClientBuilder builder = createRestClientBuilder(config);

    // Configure HTTP client
    builder.setHttpClientConfigCallback(
        httpAsyncClientBuilder -> {
          // SSL configuration
          if (config.isUseSSL()) {
            try {
              SSLContext sslContext = javax.net.ssl.SSLContext.getDefault();
              httpAsyncClientBuilder
                  .setSSLContext(sslContext)
                  .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            } catch (Exception e) {
              throw new RuntimeException("Failed to configure SSL", e);
            }
          }

          // Connection manager configuration
          try {
            httpAsyncClientBuilder.setConnectionManager(createConnectionManager(config));
          } catch (IOReactorException e) {
            throw new IllegalStateException(
                "Unable to start ElasticSearch client. Please verify connection configuration.");
          }

          // IO Reactor configuration
          log.info(
              "Configuring Elasticsearch client with threadCount: {}", config.getThreadCount());
          httpAsyncClientBuilder.setDefaultIOReactorConfig(
              IOReactorConfig.custom().setIoThreadCount(config.getThreadCount()).build());

          // Authentication
          configureAuthentication(httpAsyncClientBuilder, config);

          return httpAsyncClientBuilder;
        });

    RestClientTransport restClientTransport =
        new RestClientTransport(builder.build(), new JacksonJsonpMapper());

    return new ElasticsearchClient(restClientTransport);
  }

  private RestClientBuilder createRestClientBuilder(ShimConfiguration config) {
    String scheme = config.isUseSSL() ? "https" : "http";
    HttpHost host = new HttpHost(config.getHost(), config.getPort(), scheme);

    RestClientBuilder builder = RestClient.builder(host);

    if (!StringUtils.isEmpty(config.getPathPrefix())) {
      builder.setPathPrefix(config.getPathPrefix());
    }

    builder.setRequestConfigCallback(
        requestConfigBuilder ->
            requestConfigBuilder
                .setConnectionRequestTimeout(config.getConnectionRequestTimeout())
                .setSocketTimeout(config.getSocketTimeout()));

    return builder;
  }

  /**
   * Create connection manager with proper thread count and connection pool configuration. This
   * ensures optimal performance by matching connection pool size to thread count.
   */
  private NHttpClientConnectionManager createConnectionManager(ShimConfiguration config)
      throws IOReactorException {
    SSLContext sslContext = SSLContexts.createDefault();
    javax.net.ssl.HostnameVerifier hostnameVerifier =
        new DefaultHostnameVerifier(PublicSuffixMatcherLoader.getDefault());
    SchemeIOSessionStrategy sslStrategy =
        new SSLIOSessionStrategy(sslContext, null, null, hostnameVerifier);

    log.info(
        "Creating IOReactorConfig with threadCount: {}, socketTimeout: {}ms",
        config.getThreadCount(),
        config.getSocketTimeout());
    IOReactorConfig ioReactorConfig =
        IOReactorConfig.custom()
            .setIoThreadCount(config.getThreadCount())
            .setSoTimeout(config.getSocketTimeout())
            .build();
    DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    IOReactorExceptionHandler ioReactorExceptionHandler =
        new IOReactorExceptionHandler() {
          @Override
          public boolean handle(java.io.IOException ex) {
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
    int maxConnectionsPerRoute = Math.max(2, config.getThreadCount());
    connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);

    log.info(
        "Configured connection pool: maxPerRoute={} (threadCount={})",
        maxConnectionsPerRoute,
        config.getThreadCount());

    return connectionManager;
  }

  private void configureAuthentication(
      HttpAsyncClientBuilder httpAsyncClientBuilder, ShimConfiguration config) {
    // Basic authentication
    if (config.getUsername() != null && config.getPassword() != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
      httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }

    // AWS IAM authentication
    if (config.isUseAwsIamAuth()) {
      if (config.getRegion() == null) {
        throw new IllegalArgumentException("Region must not be null when AWS IAM auth is enabled");
      }
      // TODO: Implement AWS IAM auth interceptor for ElasticSearch
      log.warn("AWS IAM authentication not yet implemented for ElasticSearch 8.x shim");
    }
  }

  // Core search operations

  /**
   * This only maps a subset of fields intentionally as some are more complicated to map across and
   * we don't actually use them
   */
  @Nonnull
  @Override
  public SearchResponse search(
      @Nonnull SearchRequest searchRequest, @Nonnull RequestOptions options) throws IOException {
    SearchSourceBuilder searchSourceBuilder = searchRequest.source();
    Map<String, Aggregation> aggregationMap =
        convertAggregations(searchSourceBuilder.aggregations());
    List<SortOptions> sortOptions = convertSorts(searchSourceBuilder.sorts());
    SlicedScroll slicedScroll = convertSlice(searchSourceBuilder.slice());
    Highlight highlight = convertHighlights(searchSourceBuilder.highlighter());
    PointInTimeReference pointInTimeReference =
        convertPIT(searchSourceBuilder.pointInTimeBuilder());
    List<Rescore> rescores =
        Optional.ofNullable(searchSourceBuilder.rescores()).stream()
            .flatMap(List::stream)
            .map(this::convertRescore)
            .collect(Collectors.toList());
    Suggester suggester = convertSuggestions(searchSourceBuilder.suggest());
    SourceConfig sourceConfig = convertFetchSource(searchSourceBuilder.fetchSource());
    Double minScore =
        searchSourceBuilder.minScore() != null
            ? searchSourceBuilder.minScore().doubleValue()
            : null;
    co.elastic.clients.elasticsearch.core.SearchRequest.Builder esSearchRequest =
        new co.elastic.clients.elasticsearch.core.SearchRequest.Builder()
            .index(Arrays.asList(searchRequest.indices()))
            .aggregations(aggregationMap)
            .allowPartialSearchResults(searchRequest.allowPartialSearchResults())
            .explain(searchSourceBuilder.explain())
            .from(Math.max(searchSourceBuilder.from(), 0))
            .timeout(
                searchSourceBuilder.timeout() == null
                    ? null
                    : searchSourceBuilder.timeout().getStringRep())
            .version(searchSourceBuilder.version())
            .slice(slicedScroll)
            .scroll(
                searchRequest.scroll() == null
                    ? null
                    : new Time.Builder()
                        .time(searchRequest.scroll().keepAlive().getStringRep())
                        .build())
            .size(Math.max(0, searchSourceBuilder.size()))
            .highlight(highlight)
            .trackTotalHits(
                searchSourceBuilder.trackTotalHitsUpTo() == null
                    ? null
                    : new TrackHits.Builder()
                        .enabled(searchSourceBuilder.trackTotalHitsUpTo() > 0)
                        .build())
            .batchedReduceSize((long) searchRequest.getBatchedReduceSize())
            .ccsMinimizeRoundtrips(
                searchRequest.isCcsMinimizeRoundtrips() && pointInTimeReference == null)
            .maxConcurrentShardRequests((long) searchRequest.getMaxConcurrentShardRequests())
            .minScore(minScore)
            .pit(pointInTimeReference)
            .postFilter(convertQuery(searchSourceBuilder.postFilter()))
            .preFilterShardSize(
                searchRequest.getPreFilterShardSize() == null
                    ? null
                    : searchRequest.getPreFilterShardSize().longValue())
            .preference(searchRequest.preference())
            .profile(searchSourceBuilder.profile())
            .query(convertQuery(searchSourceBuilder.query()))
            .requestCache(searchRequest.requestCache())
            .routing(searchRequest.routing())
            .seqNoPrimaryTerm(searchSourceBuilder.seqNoAndPrimaryTerm())
            .terminateAfter((long) searchSourceBuilder.terminateAfter())
            .trackScores(searchSourceBuilder.trackScores())
            .suggest(suggester)
            .source(sourceConfig);
    if (searchSourceBuilder.searchAfter() != null && searchSourceBuilder.searchAfter().length > 0) {
      esSearchRequest.searchAfter(
          Arrays.stream(searchSourceBuilder.searchAfter())
              .map(value -> value == null ? FieldValue.NULL : FieldValue.of(value))
              .collect(Collectors.toList()));
    }
    if (sortOptions != null && !sortOptions.isEmpty()) {
      esSearchRequest.sort(sortOptions);
    }
    if (!rescores.isEmpty()) {
      esSearchRequest.rescore(rescores);
    }
    if (searchSourceBuilder.stats() != null && !searchSourceBuilder.stats().isEmpty()) {
      esSearchRequest.stats(searchSourceBuilder.stats());
    }

    co.elastic.clients.elasticsearch.core.SearchResponse<JsonNode> esSearchResponse =
        withTransportOptions(options).search(esSearchRequest.build(), JsonNode.class);
    String json = JsonpUtils.toJsonString(esSearchResponse, jacksonJsonpMapper);
    return SearchResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  private Map<String, Aggregation> convertAggregations(
      @Nullable AggregatorFactories.Builder aggregations) throws JsonProcessingException {
    if (aggregations == null) {
      return Collections.emptyMap();
    }
    JsonNode mappings = objectMapper.readTree(aggregations.toString());
    return mappings.properties().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    Aggregation.of(
                        prop -> {
                          try {
                            return prop.withJson(
                                jacksonJsonpMapper
                                    .jsonProvider()
                                    .createParser(
                                        new StringReader(
                                            objectMapper.writeValueAsString(entry.getValue()))),
                                jacksonJsonpMapper);
                          } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                          }
                        })));
  }

  @Nullable
  private List<SortOptions> convertSorts(List<SortBuilder<?>> sorts) {
    return sorts == null
        ? Collections.emptyList()
        : sorts.stream().map(this::convertSort).collect(Collectors.toList());
  }

  @Nullable
  private SlicedScroll convertSlice(@Nullable SliceBuilder sliceBuilder) {
    if (sliceBuilder == null) {
      return null;
    }
    SlicedScroll.Builder builder =
        new SlicedScroll.Builder()
            .id(String.valueOf(sliceBuilder.getId()))
            .max(sliceBuilder.getMax());
    // ElasticSearch doesn't allow slicing on _id
    if (!"_id".equals(sliceBuilder.getField())) {
      builder.field(sliceBuilder.getField());
    }
    return builder.build();
  }

  @Nullable
  private Highlight convertHighlights(@Nullable HighlightBuilder highlightBuilder) {
    // TODO: Fields should only be set if not empty
    if (highlightBuilder == null) {
      return null;
    }
    Highlight.Builder highlight = new Highlight.Builder();
    if (highlightBuilder.fields() != null && !highlightBuilder.fields().isEmpty()) {
      highlight.fields(
          highlightBuilder.fields().stream()
              .collect(
                  Collectors.toMap(
                      HighlightBuilder.Field::name,
                      field -> {
                        HighlightField.Builder fieldBuilder = new HighlightField.Builder();
                        fieldBuilder
                            .numberOfFragments(field.numOfFragments())
                            .numberOfFragments(field.numOfFragments())
                            .noMatchSize(field.noMatchSize())
                            .requireFieldMatch(field.requireFieldMatch())
                            .fragmentSize(field.fragmentSize())
                            .highlightFilter(field.highlightFilter());
                        if (field.preTags() != null) {
                          fieldBuilder.preTags(Arrays.asList(field.preTags()));
                        }
                        if (field.postTags() != null) {
                          fieldBuilder.postTags(Arrays.asList(field.postTags()));
                        }
                        return fieldBuilder.build();
                      })));
    }
    highlight
        .numberOfFragments(highlightBuilder.numOfFragments())
        .requireFieldMatch(highlightBuilder.requireFieldMatch())
        .fragmentSize(highlightBuilder.fragmentSize())
        .highlightFilter(highlightBuilder.highlightFilter())
        .encoder(
            HighlighterEncoder._DESERIALIZER.parse(
                Optional.ofNullable(highlightBuilder.encoder()).orElse("default")));
    if (highlightBuilder.preTags() != null) {
      highlight.preTags(Arrays.asList(highlightBuilder.preTags()));
    }
    if (highlightBuilder.postTags() != null) {
      highlight.postTags(Arrays.asList(highlightBuilder.postTags()));
    }
    return highlight.build();
  }

  @Nullable
  private PointInTimeReference convertPIT(PointInTimeBuilder pit) {
    return pit == null
        ? null
        : new PointInTimeReference.Builder()
            .id(pit.getId())
            .keepAlive(new Time.Builder().time(pit.getKeepAlive().getStringRep()).build())
            .build();
  }

  @Nullable
  private Suggester convertSuggestions(@Nullable SuggestBuilder suggestBuilder) {
    return suggestBuilder == null
        ? null
        : new Suggester.Builder()
            .text(suggestBuilder.getGlobalText())
            .suggesters(
                suggestBuilder.getSuggestions().entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, entry -> convertSuggestion(entry.getValue()))))
            .build();
  }

  @Nullable
  private SourceConfig convertFetchSource(@Nullable FetchSourceContext fetchSourceContext) {
    if (fetchSourceContext == null) {
      return null;
    }
    SourceConfig.Builder sourceConfig = new SourceConfig.Builder();
    String[] includes = ArrayUtils.nullToEmpty(fetchSourceContext.includes());
    String[] excludes = ArrayUtils.nullToEmpty(fetchSourceContext.excludes());
    if (includes.length > 0 || excludes.length > 0) {
      sourceConfig.filter(
          new SourceFilter.Builder()
              .includes(Arrays.asList(includes))
              .excludes(Arrays.asList(excludes))
              .build());
    } else {
      sourceConfig.fetch(fetchSourceContext.fetchSource());
    }
    return sourceConfig.build();
  }

  @Nonnull
  @Override
  public SearchResponse scroll(
      @Nonnull SearchScrollRequest searchScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    throw new UnsupportedOperationException("Scroll is unused, not implemented for ES8 Shim.");
  }

  @Nonnull
  @Override
  public ClearScrollResponse clearScroll(
      @Nonnull ClearScrollRequest clearScrollRequest, @Nonnull RequestOptions options)
      throws IOException {
    throw new UnsupportedOperationException("Scroll is unused, not implemented for ES8 Shim.");
  }

  @Nonnull
  @Override
  public CountResponse count(@Nonnull CountRequest countRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.core.CountRequest esCountRequest =
        new co.elastic.clients.elasticsearch.core.CountRequest.Builder()
            .index(Arrays.asList(countRequest.indices()))
            .query(convertQuery(countRequest.query()))
            .build();
    co.elastic.clients.elasticsearch.core.CountResponse esCountResponse =
        client.count(esCountRequest);
    ShardStatistics esShardStats = esCountResponse.shards();
    ShardSearchFailure[] shardFailures = convertShardFailures(esShardStats);

    return new CountResponse(
        esCountResponse.count(),
        false,
        new CountResponse.ShardStats(
            esShardStats.successful().intValue(),
            esShardStats.total().intValue(),
            Optional.ofNullable(esShardStats.skipped()).orElse(0).intValue(),
            shardFailures));
  }

  private ShardSearchFailure[] convertShardFailures(ShardStatistics esShardStats) {
    ShardSearchFailure[] shardFailures = new ShardSearchFailure[0];
    if (esShardStats == null) {
      return shardFailures;
    }
    try {
      // Try to map failures, but if we can't then at minimum log
      shardFailures =
          esShardStats.failures().stream()
              .map(
                  shardSearchFailure ->
                      new ShardSearchFailure(
                          new SearchException(
                              new SearchShardTarget(
                                  shardSearchFailure.node(),
                                  new ShardId(
                                      new Index(
                                          Optional.ofNullable(shardSearchFailure.index())
                                              .orElse(""),
                                          Optional.ofNullable(shardSearchFailure.index())
                                              .orElse("")),
                                      shardSearchFailure.shard()),
                                  null,
                                  OriginalIndices.NONE),
                              shardSearchFailure.reason().reason(),
                              null)))
              .toArray(ShardSearchFailure[]::new);
    } catch (Exception e) {
      log.error("Search failed to execute and could not map failures: {}", esShardStats.failures());
    }
    return shardFailures;
  }

  @Nonnull
  @Override
  public ExplainResponse explain(
      @Nonnull ExplainRequest explainRequest, @Nonnull RequestOptions options) throws IOException {
    co.elastic.clients.elasticsearch.core.ExplainRequest esExplainRequest =
        new co.elastic.clients.elasticsearch.core.ExplainRequest.Builder()
            .id(explainRequest.id())
            .query(convertQuery(explainRequest.query()))
            .index(explainRequest.index())
            .routing(explainRequest.routing())
            .build();
    co.elastic.clients.elasticsearch.core.ExplainResponse<JsonNode> esExplainResponse =
        withTransportOptions(options).explain(esExplainRequest, JsonNode.class);
    String json = JsonpUtils.toJsonString(esExplainResponse, jacksonJsonpMapper);
    return ExplainResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json),
        esExplainResponse.matched());
  }

  // Document operations
  @Nonnull
  @Override
  public GetResponse getDocument(@Nonnull GetRequest getRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.core.GetRequest esGetRequest =
        new co.elastic.clients.elasticsearch.core.GetRequest.Builder()
            .id(getRequest.id())
            .index(getRequest.index())
            .build();
    co.elastic.clients.elasticsearch.core.GetResponse<JsonNode> esGetResponse =
        withTransportOptions(options).get(esGetRequest, JsonNode.class);
    String json = JsonpUtils.toJsonString(esGetResponse, jacksonJsonpMapper);
    return GetResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  @Override
  public IndexResponse indexDocument(
      @Nonnull IndexRequest indexRequest, @Nonnull RequestOptions options) throws IOException {
    co.elastic.clients.elasticsearch.core.IndexRequest<JsonNode> esIndexRequest =
        new co.elastic.clients.elasticsearch.core.IndexRequest.Builder<JsonNode>()
            .index(indexRequest.index())
            .document(
                objectMapper.readTree(
                    XContentHelper.convertToJson(indexRequest.source(), true, XContentType.JSON)))
            .id(indexRequest.id())
            .build();
    co.elastic.clients.elasticsearch.core.IndexResponse indexResponse =
        withTransportOptions(options).index(esIndexRequest);
    String json = JsonpUtils.toJsonString(indexResponse, jacksonJsonpMapper);
    return IndexResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  @Override
  public DeleteResponse deleteDocument(
      @Nonnull DeleteRequest deleteRequest, @Nonnull RequestOptions options) throws IOException {
    co.elastic.clients.elasticsearch.core.DeleteRequest esDeleteRequest =
        new co.elastic.clients.elasticsearch.core.DeleteRequest.Builder()
            .id(deleteRequest.id())
            .routing(deleteRequest.routing())
            .ifPrimaryTerm(deleteRequest.ifPrimaryTerm())
            .index(deleteRequest.index())
            .ifSeqNo(deleteRequest.ifSeqNo())
            .version(deleteRequest.version())
            .build();
    co.elastic.clients.elasticsearch.core.DeleteResponse esDeleteResponse =
        withTransportOptions(options).delete(esDeleteRequest);
    String json = JsonpUtils.toJsonString(esDeleteResponse, jacksonJsonpMapper);
    return DeleteResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  @Override
  public BulkByScrollResponse deleteByQuery(
      @Nonnull DeleteByQueryRequest deleteByQueryRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.core.DeleteByQueryRequest esDeleteByQueryRequest =
        convertDeleteByQueryRequest(deleteByQueryRequest, true);
    DeleteByQueryResponse esDeleteByQueryResponse =
        withTransportOptions(options).deleteByQuery(esDeleteByQueryRequest);
    BulkByScrollTask.Status status =
        new BulkByScrollTask.Status(
            esDeleteByQueryResponse.sliceId(),
            Optional.ofNullable(esDeleteByQueryResponse.total()).orElse(0L),
            0L,
            0L,
            Optional.ofNullable(esDeleteByQueryResponse.deleted()).orElse(0L),
            Optional.ofNullable(esDeleteByQueryResponse.batches()).orElse(0L).intValue(),
            Optional.ofNullable(esDeleteByQueryResponse.versionConflicts()).orElse(0L),
            Optional.ofNullable(esDeleteByQueryResponse.noops()).orElse(0L),
            Optional.ofNullable(esDeleteByQueryResponse.retries()).map(Retries::bulk).orElse(0L),
            Optional.ofNullable(esDeleteByQueryResponse.retries()).map(Retries::search).orElse(0L),
            new TimeValue(
                Optional.ofNullable(esDeleteByQueryResponse.throttledMillis()).orElse(0L),
                TimeUnit.MILLISECONDS),
            Optional.ofNullable(esDeleteByQueryResponse.requestsPerSecond()).orElse(0f),
            null,
            new TimeValue(
                Optional.ofNullable(esDeleteByQueryResponse.throttledUntilMillis()).orElse(0L),
                TimeUnit.MILLISECONDS));
    List<BulkItemResponse.Failure> bulkFailures =
        esDeleteByQueryResponse.failures().stream()
            .map(
                failure ->
                    new BulkItemResponse.Failure(
                        failure.index(), failure.id(), new Exception(failure.cause().reason())))
            .collect(Collectors.toList());
    return new BulkByScrollResponse(
        new TimeValue(
            Optional.ofNullable(esDeleteByQueryResponse.took()).orElse(0L), TimeUnit.MILLISECONDS),
        status,
        bulkFailures,
        Collections.emptyList(),
        Optional.ofNullable(esDeleteByQueryResponse.timedOut()).orElse(false));
  }

  @Nonnull
  @Override
  public CreatePitResponse createPit(
      @Nonnull CreatePitRequest createPitRequest, @Nonnull RequestOptions options)
      throws IOException {
    OpenPointInTimeRequest esCreatePitRequest =
        new OpenPointInTimeRequest.Builder()
            .index(
                createPitRequest.indices() == null
                    ? Collections.emptyList()
                    : Arrays.asList(createPitRequest.indices()))
            .keepAlive(
                createPitRequest.getKeepAlive() == null
                    ? null
                    : new Time.Builder()
                        .time(createPitRequest.getKeepAlive().getStringRep())
                        .build())
            .routing(createPitRequest.getRouting())
            .preference(createPitRequest.getPreference())
            .build();

    // We disable required non null checks because ES-Java introduced a backwards incompatible
    // client side change
    OpenPointInTimeResponse esCreatePitResponse =
        withTransportOptions(options).openPointInTime(esCreatePitRequest);
    ShardStatistics esShardStats = esCreatePitResponse.shards();
    ShardSearchFailure[] searchFailures = convertShardFailures(esShardStats);
    return new CreatePitResponse(
        esCreatePitResponse.id(),
        System.currentTimeMillis(),
        esShardStats.total().intValue(),
        esShardStats.successful().intValue(),
        Optional.ofNullable(esShardStats.skipped()).map(Number::intValue).orElse(0),
        esShardStats.failed().intValue(),
        searchFailures);
  }

  @Nonnull
  @Override
  public DeletePitResponse deletePit(
      @Nonnull DeletePitRequest deletePitRequest, @Nonnull RequestOptions options)
      throws IOException {
    List<String> pitIds = deletePitRequest.getPitIds();
    List<DeletePitInfo> deletePitInfos = new ArrayList<>();
    for (String pitId : pitIds) {
      ClosePointInTimeRequest esDeletePitRequest =
          new ClosePointInTimeRequest.Builder().id(pitId).build();

      ClosePointInTimeResponse esDeletePitResponse =
          withTransportOptions(options).closePointInTime(esDeletePitRequest);
      DeletePitInfo deletePitInfo = new DeletePitInfo(esDeletePitResponse.succeeded(), pitId);
      deletePitInfos.add(deletePitInfo);
    }

    return new DeletePitResponse(deletePitInfos);
  }

  private co.elastic.clients.elasticsearch.core.DeleteByQueryRequest convertDeleteByQueryRequest(
      DeleteByQueryRequest deleteByQueryRequest, boolean synchronous) throws IOException {
    return new co.elastic.clients.elasticsearch.core.DeleteByQueryRequest.Builder()
        .query(convertQuery(deleteByQueryRequest.getSearchRequest().source().query()))
        .index(Arrays.asList(deleteByQueryRequest.indices()))
        .timeout(new Time.Builder().time(deleteByQueryRequest.getTimeout().getStringRep()).build())
        .refresh(deleteByQueryRequest.isRefresh())
        .scrollSize((long) deleteByQueryRequest.getBatchSize())
        .waitForCompletion(synchronous)
        .build();
  }

  // Index management operations
  @Nonnull
  @Override
  public CreateIndexResponse createIndex(
      @Nonnull CreateIndexRequest createIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.indices.CreateIndexRequest esCreateIndexRequest =
        new co.elastic.clients.elasticsearch.indices.CreateIndexRequest.Builder()
            .index(createIndexRequest.index())
            .mappings(
                createIndexRequest.mappings() != null
                    ? convertTypeMapping(createIndexRequest.mappings())
                    : null)
            .settings(
                createIndexRequest.settings() != null
                        && !Settings.EMPTY.equals(createIndexRequest.settings())
                    ? convertIndexSettings(createIndexRequest.settings())
                    : null)
            .build();
    co.elastic.clients.elasticsearch.indices.CreateIndexResponse esCreateResponse =
        withTransportOptions(options).indices().create(esCreateIndexRequest);
    return new CreateIndexResponse(
        esCreateResponse.acknowledged(),
        esCreateResponse.shardsAcknowledged(),
        esCreateResponse.index());
  }

  @Nonnull
  @Override
  public GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.indices.GetIndexRequest esGetIndexRequest =
        new co.elastic.clients.elasticsearch.indices.GetIndexRequest.Builder()
            .index(Arrays.asList(getIndexRequest.indices()))
            .includeDefaults(getIndexRequest.includeDefaults())
            .build();
    co.elastic.clients.elasticsearch.indices.GetIndexResponse esGetIndexResponse =
        withTransportOptions(options).indices().get(esGetIndexRequest);
    Map<String, IndexState> result = esGetIndexResponse.result();
    Set<String> indices = result.keySet();
    return new GetIndexResponse(
        indices.toArray(new String[0]),
        result.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertMappings(entry.getValue().mappings()))),
        result.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> convertMetadata(entry.getValue()))),
        result.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertSettings(entry.getValue().settings()))),
        result.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertSettings(entry.getValue().defaults()))),
        result.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        Optional.ofNullable(entry.getValue().dataStream())
                            .orElse(StringUtils.EMPTY))));
  }

  private List<AliasMetadata> convertMetadata(IndexState state) {
    return state.aliases().entrySet().stream()
        .map(
            entry ->
                AliasMetadata.newAliasMetadataBuilder(entry.getKey())
                    .filter(
                        Optional.ofNullable(entry.getValue().filter())
                            .map(Query::toString)
                            .orElse(null))
                    .indexRouting(entry.getValue().indexRouting())
                    .searchRouting(entry.getValue().searchRouting())
                    .writeIndex(entry.getValue().isWriteIndex())
                    .isHidden(entry.getValue().isHidden())
                    .build())
        .collect(Collectors.toList());
  }

  @Nonnull
  private Settings convertSettings(IndexSettings settings) {
    Settings.Builder builder = Settings.builder();
    if (settings == null) {
      return builder.build();
    }
    String settingsString = JsonpUtils.toJsonString(settings, jacksonJsonpMapper);
    return builder.loadFromSource(settingsString, XContentType.JSON).build();
  }

  @Nonnull
  private MappingMetadata convertMappings(TypeMapping mapping) {
    if (mapping == null) {
      return MappingMetadata.EMPTY_MAPPINGS;
    }
    String mappingAsString = JsonpUtils.toJsonString(mapping, jacksonJsonpMapper);
    try {
      return new MappingMetadata(
          "_doc", objectMapper.readValue(mappingAsString, new TypeReference<>() {}));
    } catch (Exception e) {
      log.warn("Unable to convert mapping, since this is unused continuing: " + mappingAsString);
      return MappingMetadata.EMPTY_MAPPINGS;
    }
  }

  private SortOptions convertSort(SortBuilder<?> sortBuilder) {
    String jsonString = sortBuilder.toString();
    return SortOptions._DESERIALIZER.deserialize(
        jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
        jacksonJsonpMapper);
  }

  @Nonnull
  @Override
  public ResizeResponse cloneIndex(ResizeRequest resizeRequest, RequestOptions options)
      throws IOException {
    CloneIndexRequest esCloneIndexRequest =
        new CloneIndexRequest.Builder()
            .index(resizeRequest.getSourceIndex())
            .target(resizeRequest.getTargetIndex())
            .build();
    CloneIndexResponse esCloneIndexResponse =
        withTransportOptions(options).indices().clone(esCloneIndexRequest);
    return new ResizeResponse(
        esCloneIndexResponse.acknowledged(),
        esCloneIndexResponse.shardsAcknowledged(),
        esCloneIndexResponse.index());
  }

  @Nonnull
  @Override
  public AcknowledgedResponse deleteIndex(
      @Nonnull DeleteIndexRequest deleteIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.indices.DeleteIndexRequest esDeleteIndexRequest =
        new co.elastic.clients.elasticsearch.indices.DeleteIndexRequest.Builder()
            .index(Arrays.asList(deleteIndexRequest.indices()))
            .timeout(
                new Time.Builder()
                    .time(
                        Optional.ofNullable(deleteIndexRequest.timeout())
                            .map(TimeValue::getStringRep)
                            .orElse(null))
                    .build())
            .build();
    DeleteIndexResponse esDeleteIndexResponse =
        withTransportOptions(options).indices().delete(esDeleteIndexRequest);
    return new AcknowledgedResponse(esDeleteIndexResponse.acknowledged());
  }

  @Override
  public boolean indexExists(
      @Nonnull GetIndexRequest getIndexRequest, @Nonnull RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.indices.ExistsRequest esIndexRequest =
        new co.elastic.clients.elasticsearch.indices.ExistsRequest.Builder()
            .index(Arrays.asList(getIndexRequest.indices()))
            .build();
    return withTransportOptions(options).indices().exists(esIndexRequest).value();
  }

  @Nonnull
  @Override
  public AcknowledgedResponse putIndexMapping(
      @Nonnull PutMappingRequest putMappingRequest, @Nonnull RequestOptions options)
      throws IOException {
    Map<String, Object> mappings =
        objectMapper.readValue(putMappingRequest.source().utf8ToString(), new TypeReference<>() {});
    if (mappings.containsKey("properties")) {
      Object props = mappings.get("properties");
      if (props instanceof Map) {
        mappings = (Map<String, Object>) props;
      }
    }
    Map<String, Property> propertyMap =
        mappings.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        Property.of(
                            prop -> {
                              try {
                                return prop.withJson(
                                    jacksonJsonpMapper
                                        .jsonProvider()
                                        .createParser(
                                            new StringReader(
                                                objectMapper.writeValueAsString(entry.getValue()))),
                                    jacksonJsonpMapper);
                              } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                              }
                            })));
    co.elastic.clients.elasticsearch.indices.PutMappingRequest esPutMappingRequest =
        new co.elastic.clients.elasticsearch.indices.PutMappingRequest.Builder()
            .index(Arrays.asList(putMappingRequest.indices()))
            .properties(propertyMap)
            .build();
    PutMappingResponse esPutMappingsResponse =
        withTransportOptions(options).indices().putMapping(esPutMappingRequest);
    return new AcknowledgedResponse(esPutMappingsResponse.acknowledged());
  }

  @Nonnull
  @Override
  public GetMappingsResponse getIndexMapping(
      @Nonnull GetMappingsRequest getMappingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    GetMappingRequest esGetMappingRequest =
        new GetMappingRequest.Builder().index(Arrays.asList(getMappingsRequest.indices())).build();
    GetMappingResponse esGetMappingResponse =
        withTransportOptions(options).indices().getMapping(esGetMappingRequest);
    return new GetMappingsResponse(
        esGetMappingResponse.result().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertMappings(entry.getValue().mappings()))));
  }

  @Nonnull
  @Override
  public GetSettingsResponse getIndexSettings(
      @Nonnull GetSettingsRequest getSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    GetIndicesSettingsRequest esGetSettingRequest =
        new GetIndicesSettingsRequest.Builder()
            .index(Arrays.asList(getSettingsRequest.indices()))
            .name(
                getSettingsRequest.names() != null
                    ? Arrays.asList(getSettingsRequest.names())
                    : null)
            .includeDefaults(getSettingsRequest.includeDefaults())
            .build();
    GetIndicesSettingsResponse esSettingsResponse =
        withTransportOptions(options).indices().getSettings(esGetSettingRequest);
    return new GetSettingsResponse(
        esSettingsResponse.result().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertSettings(entry.getValue().settings()))),
        esSettingsResponse.result().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> convertSettings(entry.getValue().defaults()))));
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexSettings(
      @Nonnull UpdateSettingsRequest updateSettingsRequest, @Nonnull RequestOptions options)
      throws IOException {
    PutIndicesSettingsRequest esPutSettingsRequest =
        new PutIndicesSettingsRequest.Builder()
            .index(Arrays.asList(updateSettingsRequest.indices()))
            .settings(
                updateSettingsRequest.settings() != null
                        && !Settings.EMPTY.equals(updateSettingsRequest.settings())
                    ? convertIndexSettings(updateSettingsRequest.settings())
                    : null)
            .build();
    PutIndicesSettingsResponse esUpdatedSettingsResponse =
        withTransportOptions(options).indices().putSettings(esPutSettingsRequest);
    return new AcknowledgedResponse(esUpdatedSettingsResponse.acknowledged());
  }

  @Nonnull
  @Override
  public RefreshResponse refreshIndex(
      @Nonnull RefreshRequest refreshRequest, @Nonnull RequestOptions options) throws IOException {
    co.elastic.clients.elasticsearch.indices.RefreshRequest esRefreshRequest =
        new co.elastic.clients.elasticsearch.indices.RefreshRequest.Builder()
            .index(
                refreshRequest.indices() != null ? Arrays.asList(refreshRequest.indices()) : null)
            .build();
    co.elastic.clients.elasticsearch.indices.RefreshResponse esRefreshResponse =
        withTransportOptions(options).indices().refresh(esRefreshRequest);
    return RefreshResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(
                X_CONTENT_REGISTRY,
                LoggingDeprecationHandler.INSTANCE,
                JsonpUtils.toJsonString(esRefreshResponse, jacksonJsonpMapper)));
  }

  @Nonnull
  @Override
  public GetAliasesResponse getIndexAliases(
      @Nonnull GetAliasesRequest getAliasesRequest, @Nonnull RequestOptions options)
      throws IOException {
    GetAliasRequest esGetAliasRequest =
        new GetAliasRequest.Builder()
            .name(
                getAliasesRequest.aliases() != null
                    ? Arrays.asList(getAliasesRequest.aliases())
                    : null)
            .index(
                getAliasesRequest.indices() != null
                    ? Arrays.asList(getAliasesRequest.indices())
                    : null)
            .build();
    GetAliasResponse esGetAliasResponse;
    try {
      esGetAliasResponse = withTransportOptions(options).indices().getAlias(esGetAliasRequest);
    } catch (ElasticsearchException e) {
      if (e.status() == 404) {
        esGetAliasResponse = new GetAliasResponse.Builder().build();
      } else {
        throw e;
      }
    }
    return GetAliasesResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(
                X_CONTENT_REGISTRY,
                LoggingDeprecationHandler.INSTANCE,
                JsonpUtils.toJsonString(esGetAliasResponse, jacksonJsonpMapper)));
  }

  @Nonnull
  @Override
  public AcknowledgedResponse updateIndexAliases(
      IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException {
    UpdateAliasesRequest esUpdateAliasesRequest =
        new UpdateAliasesRequest.Builder()
            .actions(
                indicesAliasesRequest.getAliasActions().stream()
                    .map(this::convertAliasAction)
                    .collect(Collectors.toList()))
            .timeout(
                indicesAliasesRequest.timeout() != null
                    ? new Time.Builder()
                        .time(indicesAliasesRequest.timeout().getStringRep())
                        .build()
                    : null)
            .build();
    UpdateAliasesResponse esUpdateAliasResponse =
        withTransportOptions(options).indices().updateAliases(esUpdateAliasesRequest);
    return new AcknowledgedResponse(esUpdateAliasResponse.acknowledged());
  }

  private Action convertAliasAction(IndicesAliasesRequest.AliasActions aliasAction) {
    String jsonString = Strings.toString(MediaTypeRegistry.JSON, aliasAction, true, true);
    return Action.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }

  @Nonnull
  @Override
  public AnalyzeResponse analyzeIndex(AnalyzeRequest request, RequestOptions options)
      throws IOException {
    co.elastic.clients.elasticsearch.indices.AnalyzeRequest esAnalyzeRequest =
        new co.elastic.clients.elasticsearch.indices.AnalyzeRequest.Builder()
            .analyzer(request.analyzer())
            .index(request.index())
            .field(request.field())
            .attributes(request.attributes() != null ? Arrays.asList(request.attributes()) : null)
            .text(request.text() != null ? Arrays.asList(request.text()) : null)
            .build();
    co.elastic.clients.elasticsearch.indices.AnalyzeResponse esAnalyzeResponse =
        withTransportOptions(options).indices().analyze(esAnalyzeRequest);
    return AnalyzeResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(
                X_CONTENT_REGISTRY,
                LoggingDeprecationHandler.INSTANCE,
                JsonpUtils.toJsonString(esAnalyzeResponse, jacksonJsonpMapper)));
  }

  @Nonnull
  @Override
  public ClusterGetSettingsResponse getClusterSettings(
      ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
      throws IOException {
    GetClusterSettingsRequest esGetClusterSettingsRequest =
        new GetClusterSettingsRequest.Builder()
            .includeDefaults(clusterGetSettingsRequest.includeDefaults())
            .build();
    GetClusterSettingsResponse esClusterSettingsResponse =
        withTransportOptions(options).cluster().getSettings(esGetClusterSettingsRequest);
    return new ClusterGetSettingsResponse(
        convertClusterSettings(esClusterSettingsResponse.persistent()),
        convertClusterSettings(esClusterSettingsResponse.transient_()),
        convertClusterSettings(esClusterSettingsResponse.defaults()));
  }

  private Settings convertClusterSettings(Map<String, JsonData> clusterSettings)
      throws IOException {
    String json = objectMapper.writeValueAsString(clusterSettings);
    return Settings.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  @Override
  public ClusterUpdateSettingsResponse putClusterSettings(
      ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
      throws IOException {
    throw new UnsupportedOperationException(
        "Not implemented currently due to no usages for the ES8 shim.");
  }

  @Nonnull
  @Override
  public ClusterHealthResponse clusterHealth(
      ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
    throw new UnsupportedOperationException(
        "Not implemented currently due to no usages for the ES8 shim.");
  }

  @Nonnull
  @Override
  public ListTasksResponse listTasks(ListTasksRequest request, RequestOptions options)
      throws IOException {
    ListRequest esListRequest = new ListRequest.Builder().detailed(request.getDetailed()).build();
    ListResponse esListResponse = withTransportOptions(options).tasks().list(esListRequest);
    String json = JsonpUtils.toJsonString(esListResponse, jacksonJsonpMapper);
    return ListTasksResponse.fromXContent(
        XContentType.JSON
            .xContent()
            .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, json));
  }

  @Nonnull
  @Override
  public Optional<GetTaskResponse> getTask(GetTaskRequest request, RequestOptions options)
      throws IOException {
    String taskId = request.getNodeId() + ":" + request.getTaskId();
    GetTasksRequest esGetTaskRequest =
        new GetTasksRequest.Builder()
            .taskId(taskId)
            .waitForCompletion(request.getWaitForCompletion())
            .timeout(
                request.getTimeout() != null
                    ? new Time.Builder().time(request.getTimeout().getStringRep()).build()
                    : null)
            .build();
    GetTasksResponse esTaskResponse = null;
    try {
      esTaskResponse = withTransportOptions(options).tasks().get(esGetTaskRequest);
    } catch (ElasticsearchException ee) {
      // ElasticSearch-Java refactored endpoints to throw exceptions instead of return optionals,
      // underlying logic
      // basically does this in HLRC
      if (ee.status() == HttpStatus.SC_NOT_FOUND) {
        return Optional.empty();
      }
      throw ee;
    }
    return Optional.of(
        GetTaskResponse.fromXContent(
            XContentType.JSON
                .xContent()
                .createParser(
                    X_CONTENT_REGISTRY,
                    LoggingDeprecationHandler.INSTANCE,
                    JsonpUtils.toJsonString(esTaskResponse, jacksonJsonpMapper))));
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
      InfoResponse info = client.info();

      Map<String, String> clusterInfo = new HashMap<>();
      clusterInfo.put("cluster_name", info.clusterName());
      clusterInfo.put("cluster_uuid", info.clusterUuid());
      clusterInfo.put("version", info.version().number());
      clusterInfo.put("build_flavor", info.version().buildFlavor());
      clusterInfo.put("build_type", info.version().buildType());
      clusterInfo.put("build_hash", info.version().buildHash());
      clusterInfo.put("lucene_version", info.version().luceneVersion());
      clusterInfo.put("engine_type", "elasticsearch");

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
      case "point_in_time":
      case "async_search":
      case "runtime_fields":
        return true;
      case "mapping_types":
        // ES 8.x removed mapping types
        return false;
      default:
        log.warn("Unknown feature requested: {}", feature);
        return false;
    }
  }

  @Nonnull
  @Override
  public RawResponse performLowLevelRequest(Request request) throws IOException {
    org.elasticsearch.client.Request esRequest =
        new org.elasticsearch.client.Request(request.getMethod(), request.getEndpoint());
    esRequest.addParameters(request.getParameters());
    esRequest.setEntity(request.getEntity());
    org.elasticsearch.client.Response esResponse =
        ((RestClientTransport) client._transport()).restClient().performRequest(esRequest);

    return new RawResponse(
        esResponse.getRequestLine(),
        esResponse.getHost(),
        esResponse.getEntity(),
        esResponse.getStatusLine());
  }

  @Nonnull
  @Override
  public BulkByScrollResponse updateByQuery(
      UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException {
    co.elastic.clients.elasticsearch.core.UpdateByQueryRequest esUpdateByQueryRequest =
        new co.elastic.clients.elasticsearch.core.UpdateByQueryRequest.Builder()
            .index(Arrays.asList(updateByQueryRequest.indices()))
            .script(convertScript(updateByQueryRequest.getScript()))
            .query(convertQuery(updateByQueryRequest.getSearchRequest().source().query()))
            .build();
    UpdateByQueryResponse esUpdateByQueryResponse =
        withTransportOptions(options).updateByQuery(esUpdateByQueryRequest);
    BulkByScrollTask.Status status =
        new BulkByScrollTask.Status(
            null,
            Optional.ofNullable(esUpdateByQueryResponse.total()).orElse(0L),
            Optional.ofNullable(esUpdateByQueryResponse.updated()).orElse(0L),
            0L,
            Optional.ofNullable(esUpdateByQueryResponse.deleted()).orElse(0L),
            Optional.ofNullable(esUpdateByQueryResponse.batches()).orElse(0L).intValue(),
            Optional.ofNullable(esUpdateByQueryResponse.versionConflicts()).orElse(0L),
            Optional.ofNullable(esUpdateByQueryResponse.noops()).orElse(0L),
            Optional.ofNullable(esUpdateByQueryResponse.retries()).map(Retries::bulk).orElse(0L),
            Optional.ofNullable(esUpdateByQueryResponse.retries()).map(Retries::search).orElse(0L),
            new TimeValue(
                Optional.ofNullable(esUpdateByQueryResponse.throttledMillis()).orElse(0L),
                TimeUnit.MILLISECONDS),
            Optional.ofNullable(esUpdateByQueryResponse.requestsPerSecond()).orElse(0f),
            null,
            new TimeValue(
                Optional.ofNullable(esUpdateByQueryResponse.throttledUntilMillis()).orElse(0L),
                TimeUnit.MILLISECONDS));
    List<BulkItemResponse.Failure> bulkFailures =
        esUpdateByQueryResponse.failures().stream()
            .map(
                failure ->
                    new BulkItemResponse.Failure(
                        failure.index(), failure.id(), new Exception(failure.cause().reason())))
            .collect(Collectors.toList());
    return new BulkByScrollResponse(
        new TimeValue(
            Optional.ofNullable(esUpdateByQueryResponse.took()).orElse(0L), TimeUnit.MILLISECONDS),
        status,
        bulkFailures,
        Collections.emptyList(),
        Optional.ofNullable(esUpdateByQueryResponse.timedOut()).orElse(false));
  }

  private Script convertScript(org.opensearch.script.Script script) {
    Script esScript = null;
    if (script != null) {
      esScript =
          new Script.Builder()
              .lang(script.getLang())
              .options(script.getOptions())
              .params(
                  script.getParams().entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey, entry -> JsonData.of(entry.getValue()))))
              .source(script.getIdOrCode())
              .build();
    }
    return esScript;
  }

  @Nonnull
  @Override
  public String submitDeleteByQueryTask(
      DeleteByQueryRequest deleteByQueryRequest, RequestOptions options) throws IOException {
    DeleteByQueryResponse deleteByQueryResponse =
        withTransportOptions(options)
            .deleteByQuery(convertDeleteByQueryRequest(deleteByQueryRequest, false));
    return Optional.ofNullable(deleteByQueryResponse.task()).orElse(StringUtils.EMPTY);
  }

  @Nonnull
  @Override
  public String submitReindexTask(ReindexRequest reindexRequest, RequestOptions options)
      throws IOException {

    Query query = null;
    if (reindexRequest.getSearchRequest().source() != null
        && reindexRequest.getSearchRequest().source().query() != null) {
      query = convertQuery(reindexRequest.getSearchRequest().source().query());
    }
    Source sourceIndex =
        new Source.Builder()
            .index(Arrays.asList(reindexRequest.getSearchRequest().indices()))
            .size(reindexRequest.getSearchRequest().source().size())
            .query(query)
            .build();
    Destination destinationIndex =
        new Destination.Builder().index(reindexRequest.getDestination().index()).build();
    Slices slices = new Slices.Builder().value(reindexRequest.getSlices()).build();
    Time time = new Time.Builder().time(reindexRequest.getTimeout().getStringRep()).build();
    co.elastic.clients.elasticsearch.core.ReindexRequest esReindexRequest =
        new co.elastic.clients.elasticsearch.core.ReindexRequest.Builder()
            .source(sourceIndex)
            .dest(destinationIndex)
            .conflicts(Conflicts.Proceed)
            .slices(slices)
            .timeout(time)
            .waitForCompletion(false)
            .build();
    ReindexResponse esReindexResponse = withTransportOptions(options).reindex(esReindexRequest);

    return Optional.ofNullable(esReindexResponse.task()).orElse(StringUtils.EMPTY);
  }

  // FIXME: Not really anything to fix, but ES8 doesn't support non-async bulk operations.
  // Everything goes through bulk
  //        with the async client
  @Override
  public void generateAsyncBulkProcessor(
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils,
      int bulkRequestsLimit,
      long bulkFlushPeriod,
      long retryInterval,
      int numRetries,
      int threadCount) {
    Supplier<BulkIngester<?>> processorSupplier =
        () -> {
          co.elastic.clients.elasticsearch._helpers.bulk.BulkListener<Object> esBulkListener =
              new Es8BulkListener(metricUtils);

          final Refresh refresh;
          switch (writeRequestRefreshPolicy) {
            case NONE:
              refresh = Refresh.False;
              break;
            case IMMEDIATE:
              refresh = Refresh.True;
              break;
            case WAIT_UNTIL:
              refresh = Refresh.WaitFor;
              break;
            default:
              refresh = null;
          }

          BulkIngester.Builder<Object> builder =
              new BulkIngester.Builder<>()
                  .client(client)
                  .flushInterval(bulkFlushPeriod, TimeUnit.SECONDS)
                  .maxOperations(bulkRequestsLimit)
                  .listener(esBulkListener);

          builder.globalSettings(new BulkRequest.Builder().refresh(refresh));
          return builder.build();
        };

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
    // ES8 uses async processors for both sync and async operations
    generateAsyncBulkProcessor(
        writeRequestRefreshPolicy,
        metricUtils,
        bulkRequestsLimit,
        bulkFlushPeriod,
        retryInterval,
        numRetries,
        threadCount);
  }

  @Override
  protected void addToProcessor(BulkIngester<?> processor, DocWriteRequest<?> writeRequest) {
    BulkOperation operation;
    if (writeRequest instanceof UpdateRequest) {
      UpdateRequest update = (UpdateRequest) writeRequest;
      Script script = convertScript(update.script());

      @SuppressWarnings("rawtypes")
      UpdateAction.Builder actionBuilder =
          new UpdateAction.Builder()
              .detectNoop(update.detectNoop())
              .docAsUpsert(update.docAsUpsert())
              .script(script)
              .upsert(update.upsert());

      // Only set doc if it exists (not present for script-only updates)
      if (update.doc() != null) {
        actionBuilder.doc(
            XContentHelper.convertToMap(update.doc().source(), true, XContentType.JSON).v2());
      }

      operation =
          new BulkOperation(
              new UpdateOperation.Builder<>()
                  .id(writeRequest.id())
                  .ifSeqNo(writeRequest.ifSeqNo())
                  .ifPrimaryTerm(writeRequest.ifPrimaryTerm())
                  .retryOnConflict(((UpdateRequest) writeRequest).retryOnConflict())
                  .requireAlias(writeRequest.isRequireAlias())
                  .index(writeRequest.index())
                  .routing(writeRequest.routing())
                  .action(actionBuilder.build())
                  .build());
    } else if (writeRequest instanceof DeleteRequest) {
      DeleteRequest deleteRequest = (DeleteRequest) writeRequest;
      operation =
          new BulkOperation(
              new DeleteOperation.Builder()
                  .ifSeqNo(writeRequest.ifSeqNo())
                  .ifPrimaryTerm(writeRequest.ifPrimaryTerm())
                  .index(writeRequest.index())
                  .routing(writeRequest.routing())
                  .id(deleteRequest.id())
                  .build());
    } else { // writeRequest instanceof IndexRequest
      IndexRequest indexRequest = (IndexRequest) writeRequest;
      operation =
          new BulkOperation(
              new IndexOperation.Builder<>()
                  .ifSeqNo(writeRequest.ifSeqNo())
                  .ifPrimaryTerm(writeRequest.ifPrimaryTerm())
                  .requireAlias(writeRequest.isRequireAlias())
                  .index(writeRequest.index())
                  .routing(writeRequest.routing())
                  .id(indexRequest.id())
                  .document(
                      XContentHelper.convertToMap(indexRequest.source(), true, XContentType.JSON)
                          .v2())
                  .build());
    }
    processor.add(operation);
  }

  @Override
  protected void flushProcessor(BulkIngester<?> processor) {
    processor.flush();
  }

  @Override
  protected void closeProcessor(BulkIngester<?> processor) {
    processor.close();
  }

  @Override
  @Nonnull
  public ElasticsearchClient getNativeClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    log.debug("Closing ES 8.x client shim");
    client.shutdown();
  }

  private ElasticsearchClient withTransportOptions(RequestOptions requestOptions) {
    if (RequestOptions.DEFAULT.equals(requestOptions)) {
      return client;
    }
    HeaderMap headerMap =
        new HeaderMap(
            requestOptions.getHeaders().stream()
                .collect(Collectors.toMap(Header::getName, Header::getValue)));
    TransportOptions transportOptions =
        new DefaultTransportOptions(headerMap, Collections.emptyMap(), null);
    return client.withTransportOptions(transportOptions);
  }

  private Query convertQuery(org.opensearch.index.query.QueryBuilder osQuery) {
    if (osQuery == null) {
      return null;
    }
    String jsonString = osQuery.toString();
    return Query.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }

  private Rescore convertRescore(RescorerBuilder<?> rescorerBuilder) {
    String jsonString = rescorerBuilder.toString();
    return Rescore.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }

  private FieldSuggester convertSuggestion(SuggestionBuilder<?> suggestionBuilder) {
    String jsonString = Strings.toString(MediaTypeRegistry.JSON, suggestionBuilder, true, true);
    return FieldSuggester.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }

  private TypeMapping convertTypeMapping(BytesReference mappings) {
    String jsonString = mappings.utf8ToString();
    return TypeMapping.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }

  private IndexSettings convertIndexSettings(Settings settings) {
    String jsonString = settings.toString();
    return IndexSettings.of(
        q ->
            q.withJson(
                jacksonJsonpMapper.jsonProvider().createParser(new StringReader(jsonString)),
                jacksonJsonpMapper));
  }
}
