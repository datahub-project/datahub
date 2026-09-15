package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchClusterIndexSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Assembles the {@link SearchClusterRegistry}: for every configured cluster, its effective
 * configuration, client, bulk processor and index builder.
 *
 * <p>Effective configuration is {@code merge(elasticsearch.* shared defaults, clusters.<name>
 * overlay)}. Primary is not treated as a defaults bucket — a secondary cluster with no overlay
 * inherits the shared defaults, never primary's overrides.
 */
@Slf4j
@Configuration
public class SearchClusterRegistryFactory {

  @Bean(name = "searchClusterRegistry")
  @Nonnull
  public SearchClusterRegistry getInstance(
      final ConfigurationProvider configurationProvider,
      @Nullable final SearchClientShims shims,
      @Qualifier("searchClientShim") final SearchClientShim<?> primaryShim,
      @Qualifier("elasticSearchIndexSettingsOverrides")
          final Map<String, Map<String, String>> indexSettingOverrides,
      final GitVersion gitVersion,
      final ObjectMapper objectMapper,
      final MetricUtils metricUtils) {

    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();

    // Contexts that wire only a single client (notably narrow Spring tests) still get a working
    // registry with everything routed to primary.
    Map<String, SearchClientShim<?>> clients =
        shims == null
            ? Map.of(ElasticSearchConfiguration.PRIMARY_CLUSTER, primaryShim)
            : shims.byCluster();

    for (Map.Entry<String, SearchClientShim<?>> entry : clients.entrySet()) {
      String clusterName = entry.getKey();
      SearchClientShim<?> client = entry.getValue();
      SearchClusterSettings cluster = esConfig.getCluster(clusterName);

      ElasticSearchConfiguration clusterConfig =
          effectiveConfig(objectMapper, esConfig, clusterName);

      ESBulkProcessor bulkProcessor =
          ElasticSearchBulkProcessorFactory.build(
              client,
              clusterConfig.getBulkProcessor(),
              cluster.getThreadCount() == null
                  ? esConfig.getThreadCount()
                  : cluster.getThreadCount(),
              metricUtils);

      ESIndexBuilder indexBuilder =
          new ESIndexBuilder(
              client,
              clusterConfig,
              configurationProvider.getStructuredProperties(),
              indexSettingOverrides,
              gitVersion);

      connections.put(
          clusterName,
          new SearchClusterRegistry.ClusterConnection(
              clusterName, clusterConfig, client, bulkProcessor, indexBuilder));
    }

    log.info(
        "Search cluster registry initialized with clusters {} and routing {}",
        connections.keySet(),
        esConfig.getComponentCluster());

    return new SearchClusterRegistry(esConfig, connections);
  }

  /**
   * Shared defaults with this cluster's overlays applied. Sizing always comes from the cluster;
   * bulk and build-indices overlays are sparse and optional.
   */
  @Nonnull
  static ElasticSearchConfiguration effectiveConfig(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull ElasticSearchConfiguration esConfig,
      @Nonnull String clusterName) {
    SearchClusterSettings cluster = esConfig.getCluster(clusterName);

    BulkProcessorConfiguration bulkProcessor =
        SearchClusterOverlay.apply(
            objectMapper, esConfig.getBulkProcessor(), cluster.getBulkProcessor());
    BulkDeleteConfiguration bulkDelete =
        SearchClusterOverlay.apply(objectMapper, esConfig.getBulkDelete(), cluster.getBulkDelete());
    BuildIndicesConfiguration buildIndices =
        SearchClusterOverlay.apply(
            objectMapper, esConfig.getBuildIndices(), cluster.getBuildIndices());

    return esConfig.toBuilder()
        .index(cluster.effectiveIndex(esConfig.getIndex()))
        .entityIndex(withClusterMappingFiles(esConfig.getEntityIndex(), cluster.getIndex()))
        .bulkProcessor(bulkProcessor)
        .bulkDelete(bulkDelete)
        .buildIndices(buildIndices)
        .build();
  }

  /**
   * Analyzer and mapping files live on {@code entityIndex.v2/v3}. A cluster overlay that sets them
   * is copied onto that cluster's effective entity-index versions without mutating the shared
   * defaults.
   */
  @Nullable
  static EntityIndexConfiguration withClusterMappingFiles(
      @Nullable EntityIndexConfiguration entityIndex,
      @Nullable SearchClusterIndexSettings overlay) {
    if (overlay == null
        || (overlay.getAnalyzerConfig() == null && overlay.getMappingConfig() == null)) {
      return entityIndex;
    }
    EntityIndexConfiguration base =
        entityIndex == null ? EntityIndexConfiguration.builder().build() : entityIndex;
    return base.toBuilder()
        .v2(overlayVersion(base.getV2(), overlay))
        .v3(overlayVersion(base.getV3(), overlay))
        .build();
  }

  @Nonnull
  private static EntityIndexVersionConfiguration overlayVersion(
      @Nullable EntityIndexVersionConfiguration version,
      @Nonnull SearchClusterIndexSettings overlay) {
    EntityIndexVersionConfiguration.EntityIndexVersionConfigurationBuilder builder =
        version == null ? EntityIndexVersionConfiguration.builder() : version.toBuilder();
    if (overlay.getAnalyzerConfig() != null) {
      builder.analyzerConfig(overlay.getAnalyzerConfig());
    }
    if (overlay.getMappingConfig() != null) {
      builder.mappingConfig(overlay.getMappingConfig());
    }
    return builder.build();
  }
}
