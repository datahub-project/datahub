package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring factory for creating SearchClientShim instances based on DataHub configuration. This
 * factory bridges DataHub's existing configuration system with the new shim architecture.
 */
@Slf4j
@Configuration
public class SearchClientShimFactory {

  // New shim-specific configuration properties
  @Value("${elasticsearch.shim.engineType:AUTO_DETECT}")
  private String shimEngineType;

  @Value("${elasticsearch.shim.autoDetectEngine:true}")
  private boolean shimAutoDetectEngine;

  @Autowired private ConfigurationProvider configurationProvider;

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
            .withPathPrefix(esConfig.getPathPrefix())
            .withAwsIamAuth(esConfig.isOpensearchUseAwsIamAuth(), esConfig.getRegion())
            .withThreadCount(esConfig.getThreadCount())
            .withConnectionRequestTimeout(esConfig.getConnectionRequestTimeout())
            .withSocketTimeout(esConfig.getSocketTimeout());

    // Determine how to create the shim
    if (shimAutoDetectEngine) {
      log.info("Auto-detecting search engine type for shim");
      return SearchClientShimUtil.createShimWithAutoDetection(configBuilder.build(), objectMapper);
    } else {
      // Parse the configured engine type
      SearchClientShim.SearchEngineType engineType = parseEngineType(shimEngineType);
      configBuilder.withEngineType(engineType);

      log.info("Creating shim with configured engine type: {}", engineType);
      return SearchClientShimUtil.createShim(configBuilder.build(), objectMapper);
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
