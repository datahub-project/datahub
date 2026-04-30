package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.ElasticsearchSSLContextFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es7CompatibilitySearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Spring factory for creating SearchClientShim instances based on DataHub configuration. This
 * factory bridges DataHub's existing configuration system with the new shim architecture.
 */
@Slf4j
@Configuration
@Import({ElasticsearchSSLContextFactory.class})
public class SearchClientShimFactory {

  // New shim-specific configuration properties
  @Value("${elasticsearch.shim.engineType:AUTO_DETECT}")
  private String shimEngineType;

  @Value("${elasticsearch.shim.autoDetectEngine:true}")
  private boolean shimAutoDetectEngine;

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired
  @Qualifier("elasticSearchSSLContext")
  private SSLContext elasticSearchSSLContext;

  /**
   * Create the SearchClientShim bean. This can be configured to either: 1. Auto-detect the search
   * engine type by connecting to the cluster 2. Use a specific engine type as configured in
   * properties
   */
  @Bean(name = "searchClientShim")
  @Nonnull
  public SearchClientShim<?> createSearchClientShim(ObjectMapper objectMapper) throws IOException {
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();

    // Build the shim configuration from DataHub configuration
    ShimConfigurationBuilder configBuilder =
        new ShimConfigurationBuilder()
            .withHost(esConfig.getHost())
            .withPort(esConfig.getPort())
            .withCredentials(esConfig.getUsername(), esConfig.getPassword())
            .withSSL(esConfig.isUseSSL())
            .withSSLContext(elasticSearchSSLContext)
            .withPathPrefix(esConfig.getPathPrefix())
            .withAwsIamAuth(esConfig.isOpensearchUseAwsIamAuth(), esConfig.getRegion())
            .withThreadCount(esConfig.getThreadCount())
            .withConnectionRequestTimeout(esConfig.getConnectionRequestTimeout())
            .withSocketTimeout(esConfig.getSocketTimeout());

    // Determine how to create the shim
    SearchClientShim<?> shim;
    if (shimAutoDetectEngine) {
      log.info("Auto-detecting search engine type for shim");
      shim = SearchClientShimUtil.createShimWithAutoDetection(configBuilder.build(), objectMapper);
    } else {
      // Parse the configured engine type
      SearchClientShim.SearchEngineType engineType = parseEngineType(shimEngineType);
      configBuilder.withEngineType(engineType);

      log.info("Creating shim with configured engine type: {}", engineType);
      shim = SearchClientShimUtil.createShim(configBuilder.build(), objectMapper);
    }

    // If semantic search is enabled on an ES 8 shim, verify the cluster meets the 8.18+ minimum.
    // This fails fast at startup rather than at query time.
    boolean semanticEnabled =
        esConfig.getEntityIndex() != null
            && esConfig.getEntityIndex().getSemanticSearch() != null
            && esConfig.getEntityIndex().getSemanticSearch().isEnabled();

    if (semanticEnabled && shim instanceof Es8SearchClientShim) {
      log.info(
          "Semantic search enabled with ES 8 shim — verifying cluster version meets 8.18+ requirement");
      ((Es8SearchClientShim) shim).verifySemanticSearchSupport();
    }

    assertCompatModeNotSemanticEnabled(shim, semanticEnabled);

    return shim;
  }

  /**
   * Fails fast if semantic search is enabled while the shim is in ES 7 compatibility mode. Called
   * at startup so misconfigurations surface immediately rather than at query time.
   *
   * <p>Package-private for direct invocation by unit tests.
   */
  static void assertCompatModeNotSemanticEnabled(
      @Nonnull SearchClientShim<?> shim, boolean semanticEnabled) {
    if (semanticEnabled && shim instanceof Es7CompatibilitySearchClientShim) {
      throw new IllegalStateException(
          "Elasticsearch 8.18+ required for semantic search; cluster is in ES 7 compatibility mode. "
              + "Upgrade the cluster or set semanticSearch.enabled=false.");
    }
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
        return SearchClientShim.SearchEngineType.ELASTICSEARCH_7;
      case "ELASTICSEARCH_8":
      case "ES8":
        return SearchClientShim.SearchEngineType.ELASTICSEARCH_8;
      case "ELASTICSEARCH_9":
      case "ES9":
        return SearchClientShim.SearchEngineType.ELASTICSEARCH_9;
      case "OPENSEARCH_2":
      case "OS2":
        return SearchClientShim.SearchEngineType.OPENSEARCH_2;
      default:
        throw new IllegalArgumentException(
            "Unsupported engine type: "
                + engineTypeStr
                + ". Supported types: ELASTICSEARCH_7, ELASTICSEARCH_8, ELASTICSEARCH_9, OPENSEARCH_2");
    }
  }
}
