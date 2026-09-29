package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.common.ElasticsearchSSLContextFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MaeConsumerConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.HttpProxySettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchClusterUri;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.ShimSettings;
import com.linkedin.metadata.config.search.SslContextSettings;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Spring factory for creating {@link SearchClientShim} instances, one per configured search
 * cluster.
 *
 * <p>Each cluster resolves its own engine type: a secondary cluster never inherits the primary's
 * {@code engineType}, since the reason to add a connection is usually that it runs a different
 * engine version. Clusters whose endpoint and credentials are identical share a single HTTP client
 * rather than opening a redundant connection pool.
 */
@Slf4j
@Configuration
@Import({ElasticsearchSSLContextFactory.class, AwsClientFactory.class})
public class SearchClientShimFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  @Qualifier("defaultAwsCredentialsProvider")
  private AwsCredentialsProvider defaultAwsCredentialsProvider;

  /**
   * Shims for every configured cluster, keyed by cluster name.
   *
   * <p>Uses {@code elasticsearch.*} timeouts, merged with {@code maeConsumer.elasticsearch.*} via
   * {@code Math.max(global, mae)} when {@code maeConsumer.enabled=true} so MAE indexing and GMS
   * share one client.
   */
  @Bean(name = "searchClientShims")
  @Nonnull
  public SearchClientShims createSearchClientShims(ObjectMapper objectMapper) throws IOException {
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    MaeConsumerConfiguration mae = configurationProvider.getMaeConsumer();
    int socketMs = mergeRestClientSocketTimeoutMs(esConfig.getSocketTimeout(), mae);
    int connMs =
        mergeRestClientConnectionRequestTimeoutMs(esConfig.getConnectionRequestTimeout(), mae);
    if (Boolean.TRUE.equals(mae != null ? mae.getEnabled() : null)
        && mae.getElasticsearch() != null) {
      log.info(
          "searchClientShim: merged MAE RestClient timeouts socketTimeoutMs={} connectionRequestTimeoutMs={}",
          socketMs,
          connMs);
    }

    Map<String, SearchClientShim<?>> shims = new LinkedHashMap<>();
    Map<String, SearchClientShim<?>> byIdentity = new LinkedHashMap<>();

    for (Map.Entry<String, SearchClusterSettings> entry : esConfig.resolvedClusters().entrySet()) {
      String clusterName = entry.getKey();
      SearchClusterSettings cluster = entry.getValue();
      if (!cluster.isConfigured()) {
        log.info(
            "Search cluster '{}' has no uri configured; skipping client creation", clusterName);
        continue;
      }

      String identity = connectionIdentity(clusterName, cluster);
      SearchClientShim<?> existing = byIdentity.get(identity);
      if (existing != null) {
        // Same endpoint and credentials: reuse the connection pool instead of doubling it.
        log.info(
            "Search cluster '{}' resolves to an already-connected endpoint; reusing that client",
            clusterName);
        shims.put(clusterName, existing);
        continue;
      }

      SearchClientShim<?> shim =
          buildSearchClientShim(objectMapper, esConfig, clusterName, cluster, socketMs, connMs);
      shims.put(clusterName, shim);
      byIdentity.put(identity, shim);
    }

    return new SearchClientShims(shims);
  }

  /** The primary cluster's client. Bean name unchanged so existing injection points still work. */
  @Bean(name = "searchClientShim")
  @Nonnull
  public SearchClientShim<?> createSearchClientShim(final SearchClientShims shims) {
    SearchClientShim<?> primary = shims.get(ElasticSearchConfiguration.PRIMARY_CLUSTER);
    if (primary == null) {
      throw new IllegalStateException(
          "elasticsearch.clusters.primary must be configured with a uri (or legacy ELASTICSEARCH_HOST)");
    }
    return primary;
  }

  /**
   * Identity used to decide whether two cluster entries are really the same connection. Includes
   * every client-affecting setting. Secrets are hashed so the key can be logged without leaking
   * passwords.
   */
  static String connectionIdentity(
      @Nonnull String clusterName, @Nonnull SearchClusterSettings cluster) {
    SearchClusterUri uri = cluster.parsedUri(clusterName);
    ShimSettings shim = cluster.getShim();
    SslContextSettings ssl = cluster.getSslContext();
    return String.join(
        "|",
        uri.normalized(),
        String.valueOf(cluster.getUsername()),
        hashedSecret(cluster.getPassword()),
        String.valueOf(cluster.isOpensearchUseAwsIamAuth()),
        String.valueOf(cluster.getRegion()),
        shim == null ? "auto" : String.valueOf(shim.getEngineType()),
        shim == null ? "auto" : String.valueOf(shim.getAutoDetectEngine()),
        sslIdentity(ssl),
        proxyIdentity(cluster.getProxy()));
  }

  @Nonnull
  private static String sslIdentity(@Nullable SslContextSettings ssl) {
    if (ssl == null || ssl.isEmpty()) {
      return "ssl:default";
    }
    return String.join(
        ",",
        String.valueOf(ssl.getProtocol()),
        String.valueOf(ssl.getSecureRandomImplementation()),
        String.valueOf(ssl.getTrustStoreFile()),
        String.valueOf(ssl.getTrustStoreType()),
        hashedSecret(ssl.getTrustStorePassword()),
        String.valueOf(ssl.getKeyStoreFile()),
        String.valueOf(ssl.getKeyStoreType()),
        hashedSecret(ssl.getKeyStorePassword()),
        hashedSecret(ssl.getKeyPassword()));
  }

  @Nonnull
  private static String proxyIdentity(@Nullable HttpProxySettings proxy) {
    if (proxy == null) {
      return "proxy:system";
    }
    if (proxy.getHost() != null) {
      return String.join(
          ",",
          proxy.getHost(),
          String.valueOf(proxy.getPort()),
          String.valueOf(proxy.getScheme()),
          String.valueOf(proxy.getUsername()),
          hashedSecret(proxy.getPassword()));
    }
    return proxy.isUseSystemProxyProperties() ? "proxy:system" : "proxy:none";
  }

  @Nonnull
  private static String hashedSecret(@Nullable String secret) {
    return Integer.toHexString(Objects.hashCode(secret));
  }

  /**
   * Combines {@code elasticsearch.socketTimeout} with optional MAE {@code socketTimeoutMs} when
   * {@code maeConsumer.enabled=true}. Package-private for unit tests.
   */
  static int mergeRestClientSocketTimeoutMs(
      int elasticsearchSocketMs, MaeConsumerConfiguration mae) {
    int result = elasticsearchSocketMs;
    if (Boolean.TRUE.equals(mae != null ? mae.getEnabled() : null)
        && mae != null
        && mae.getElasticsearch() != null) {
      Integer so = mae.getElasticsearch().getSocketTimeoutMs();
      if (so != null && so >= 0) {
        result = Math.max(result, so);
      }
    }
    return result;
  }

  /**
   * Combines {@code elasticsearch.connectionRequestTimeout} with optional MAE {@code
   * connectionRequestTimeoutMs} when {@code maeConsumer.enabled=true}. Package-private for unit
   * tests.
   */
  static int mergeRestClientConnectionRequestTimeoutMs(
      int elasticsearchConnectionRequestTimeoutMs, MaeConsumerConfiguration mae) {
    int result = elasticsearchConnectionRequestTimeoutMs;
    if (Boolean.TRUE.equals(mae != null ? mae.getEnabled() : null)
        && mae != null
        && mae.getElasticsearch() != null) {
      Integer cr = mae.getElasticsearch().getConnectionRequestTimeoutMs();
      if (cr != null && cr >= 0) {
        result = Math.max(result, cr);
      }
    }
    return result;
  }

  private SearchClientShim<?> buildSearchClientShim(
      ObjectMapper objectMapper,
      ElasticSearchConfiguration esConfig,
      String clusterName,
      SearchClusterSettings cluster,
      int socketTimeoutMs,
      int connectionRequestTimeoutMs)
      throws IOException {

    assertIamAuthHasSharedCredentials(clusterName, cluster, defaultAwsCredentialsProvider);

    SearchClusterUri uri = cluster.parsedUri(clusterName);
    SSLContext sslContext = ElasticsearchSSLContextFactory.buildSSLContext(cluster.getSslContext());

    ShimConfigurationBuilder configBuilder =
        new ShimConfigurationBuilder()
            .withHost(uri.getHost())
            .withPort(uri.getPort())
            .withCredentials(cluster.getUsername(), cluster.getPassword())
            .withSSL(uri.isUseSSL())
            .withSSLContext(sslContext)
            .withPathPrefix(uri.getPathPrefix())
            .withAwsIamAuth(cluster.isOpensearchUseAwsIamAuth(), cluster.getRegion())
            .withAwsCredentialsProvider(defaultAwsCredentialsProvider)
            .withThreadCount(
                cluster.getThreadCount() == null
                    ? esConfig.getThreadCount()
                    : cluster.getThreadCount())
            .withConnectionRequestTimeout(connectionRequestTimeoutMs)
            .withSocketTimeout(socketTimeoutMs);

    HttpProxySettings.resolve(cluster.getProxy(), uri.getHost(), uri.isUseSSL())
        .ifPresent(
            proxy ->
                configBuilder.withHttpProxy(
                    proxy.getHost(),
                    proxy.getPort(),
                    proxy.getScheme(),
                    proxy.getUsername(),
                    proxy.getPassword()));

    ShimSettings shim = cluster.getShim();
    SearchClientShim<?> client;
    if (shim == null || shim.isAutoDetectEnabled()) {
      log.info(
          "Auto-detecting search engine type for cluster '{}' at {}",
          clusterName,
          uri.normalized());
      client =
          SearchClientShimUtil.createShimWithAutoDetection(configBuilder.build(), objectMapper);
    } else {
      SearchClientShim.SearchEngineType engineType = parseEngineType(shim.getEngineType());
      configBuilder.withEngineType(engineType);
      log.info(
          "Creating shim for cluster '{}' with configured engine type: {}",
          clusterName,
          engineType);
      client = SearchClientShimUtil.createShim(configBuilder.build(), objectMapper);
    }

    // Semantic search has an engine floor, and it is checked against whichever cluster actually
    // serves it rather than against primary.
    boolean semanticEnabled =
        esConfig.getEntityIndex() != null
            && esConfig.getEntityIndex().getSemanticSearch() != null
            && esConfig.getEntityIndex().getSemanticSearch().isEnabled()
            && clusterName.equals(
                esConfig.getComponentCluster().clusterFor(SearchComponent.SEMANTIC));

    if (semanticEnabled && client instanceof Es8SearchClientShim) {
      log.info(
          "Semantic search enabled with ES 8 shim — verifying cluster version meets 8.18+ requirement");
      ((Es8SearchClientShim) client).verifySemanticSearchSupport();
    }

    assertNoNmslibOnOpenSearch3(client, esConfig, semanticEnabled);

    return client;
  }

  /**
   * OpenSearch IAM signing must use the process-wide {@code defaultAwsCredentialsProvider}. A null
   * provider would either fail later in the shim or (worse) let a client fall through to a new IRSA
   * default chain.
   */
  static void assertIamAuthHasSharedCredentials(
      @Nonnull String clusterName,
      @Nonnull SearchClusterSettings cluster,
      @Nullable AwsCredentialsProvider defaultAwsCredentialsProvider) {
    if (cluster.isOpensearchUseAwsIamAuth() && defaultAwsCredentialsProvider == null) {
      throw new IllegalStateException(
          "Shared DefaultCredentialsProvider is required when elasticsearch.clusters."
              + clusterName
              + ".opensearchUseAwsIamAuth is enabled");
    }
  }

  /**
   * OpenSearch 3.x refuses to create new indexes with the {@code nmslib} kNN engine, so a semantic
   * index configured for it would only fail later, at index-build time, with an engine error. Fail
   * at startup with an actionable message instead. Every configured model is checked, not only the
   * active one, because mappings are created for all of them.
   *
   * <p>Package-private for direct invocation by unit tests.
   */
  static void assertNoNmslibOnOpenSearch3(
      @Nonnull SearchClientShim<?> shim,
      @Nonnull ElasticSearchConfiguration esConfig,
      boolean semanticEnabled) {
    if (!semanticEnabled
        || shim.getEngineType() != SearchClientShim.SearchEngineType.OPENSEARCH_3
        || esConfig.getEntityIndex().getSemanticSearch().getModels() == null) {
      return;
    }
    esConfig
        .getEntityIndex()
        .getSemanticSearch()
        .getModels()
        .forEach(
            (modelKey, model) -> {
              if (model != null
                  && model.getKnnEngine() != null
                  && "nmslib".equalsIgnoreCase(model.getKnnEngine().trim())) {
                throw new IllegalStateException(
                    "semanticSearch.models."
                        + modelKey
                        + ".knnEngine nmslib is not supported on OpenSearch 3.x (new nmslib"
                        + " indexes are rejected); use faiss or lucene, or set"
                        + " semanticSearch.enabled=false.");
              }
            });
  }

  /** Parse the engine type from string configuration */
  private SearchClientShim.SearchEngineType parseEngineType(String engineTypeStr) {
    if (engineTypeStr == null || engineTypeStr.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Engine type must be specified when auto-detection is disabled");
    }

    switch (engineTypeStr.toUpperCase()) {
      case "AUTO_DETECT":
        throw new IllegalArgumentException(
            "AUTO_DETECT engine type requires shimAutoDetectEngine=true");
      case "ELASTICSEARCH_7":
      case "ES7":
        throw new IllegalArgumentException(
            "Elasticsearch 7.x is no longer supported as a DataHub search backend. Upgrade the"
                + " cluster to Elasticsearch 8+ or OpenSearch 2+ and set"
                + " ELASTICSEARCH_SHIM_ENGINE_TYPE to ELASTICSEARCH_8, ELASTICSEARCH_9,"
                + " OPENSEARCH_2, OPENSEARCH_3, or AUTO_DETECT.");
      case "ELASTICSEARCH_8":
      case "ES8":
        return SearchClientShim.SearchEngineType.ELASTICSEARCH_8;
      case "ELASTICSEARCH_9":
      case "ES9":
        return SearchClientShim.SearchEngineType.ELASTICSEARCH_9;
      case "OPENSEARCH_2":
      case "OS2":
        return SearchClientShim.SearchEngineType.OPENSEARCH_2;
      case "OPENSEARCH_3":
      case "OS3":
        return SearchClientShim.SearchEngineType.OPENSEARCH_3;
      default:
        throw new IllegalArgumentException(
            "Unsupported engine type: "
                + engineTypeStr
                + ". Supported types: ELASTICSEARCH_8, ELASTICSEARCH_9, OPENSEARCH_2,"
                + " OPENSEARCH_3");
    }
  }
}
