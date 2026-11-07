package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2SemanticSearchSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SettingsBuilderFactory {

  @Bean("legacySettingsBuilder")
  @ConditionalOnExpression(
      "${elasticsearch.entityIndex.v2.enabled:false} or ${elasticsearch.entityIndex.v3.enabled:false}")
  @Nonnull
  protected SettingsBuilder createLegacySettingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    IndexConfiguration indexConfig =
        searchClusterRegistry.configFor(SearchComponent.SEARCH_V2).getIndex();
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    log.info(
        "Creating LegacySettingsBuilder bean (engineType={} is diagnostic only; V2 settings are engine-agnostic)",
        v2Client.getEngineType());
    return new V2LegacySettingsBuilder(
        indexConfig,
        indexConvention,
        configProvider.getElasticSearch().getSearch().isEnableSearchV2_5(),
        entityRegistry);
  }

  @Bean("multiEntitySettingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected SettingsBuilder createMultiEntitySettingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry) {
    EntityIndexConfiguration entityIndexConfig =
        searchClusterRegistry.configFor(SearchComponent.SEARCH_V3).getEntityIndex();
    SemanticSearchConfiguration semanticConfig = entityIndexConfig.getSemanticSearch();
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    log.info(
        "Creating MultiEntitySettingsBuilder bean (engineType={}; knn on semantic-enabled V3 entity indices for OpenSearch)",
        v3Client.getEngineType());
    try {
      return new MultiEntitySettingsBuilder(
          entityIndexConfig, indexConvention, v3Client, semanticConfig);
    } catch (IOException e) {
      log.error("Failed to initialize MultiEntitySettingsBuilder", e);
      throw new RuntimeException("Failed to initialize MultiEntitySettingsBuilder", e);
    }
  }

  // Created when a hold might turn V2 on. Assembled below only while the live V2 flag is on.
  // With that flag off, semantic reads come from V3.
  @Bean("semanticSearchSettingsBuilder")
  @ConditionalOnExpression(
      "${elasticsearch.entityIndex.semanticSearch.enabled:false} and"
          + " (${elasticsearch.entityIndex.v2.enabled:false} or"
          + " ${elasticsearch.entityIndex.v3.enabled:false})")
  @Nonnull
  protected SettingsBuilder createSemanticSearchSettingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    ElasticSearchConfiguration semanticClusterConfig =
        searchClusterRegistry.configFor(SearchComponent.SEMANTIC);
    SemanticSearchConfiguration semanticConfig =
        semanticClusterConfig.getEntityIndex().getSemanticSearch();

    SearchClientShim<?> semanticClient = searchClusterRegistry.clientFor(SearchComponent.SEMANTIC);
    log.info(
        "Creating SemanticSearchSettingsBuilder bean for entities: {} engine: {}",
        semanticConfig.getEnabledEntities(),
        semanticClient.getEngineType());
    // Build V2-shaped settings from this cluster's index config so a sidecar tokenizer is not
    // inherited from the SEARCH_V2 cluster.
    return new V2SemanticSearchSettingsBuilder(
        indexConvention,
        new V2LegacySettingsBuilder(
            semanticClusterConfig.getIndex(),
            indexConvention,
            configProvider.getElasticSearch().getSearch().isEnableSearchV2_5(),
            entityRegistry),
        semanticClient);
  }

  @Bean("settingsBuilder")
  protected SettingsBuilder getInstance(
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Qualifier("legacySettingsBuilder") @Nullable SettingsBuilder legacySettingsBuilder,
      @Qualifier("multiEntitySettingsBuilder") @Nullable SettingsBuilder multiEntitySettingsBuilder,
      @Qualifier("semanticSearchSettingsBuilder") @Nullable
          SettingsBuilder semanticSearchSettingsBuilder) {
    List<SettingsBuilder> builders = new ArrayList<>();
    boolean v2Enabled =
        elasticSearchConfiguration.getEntityIndex() != null
            && elasticSearchConfiguration.getEntityIndex().getV2() != null
            && elasticSearchConfiguration.getEntityIndex().getV2().isEnabled();

    if (v2Enabled && legacySettingsBuilder != null) {
      builders.add(legacySettingsBuilder);
    }

    if (multiEntitySettingsBuilder != null) {
      builders.add(multiEntitySettingsBuilder);
    }

    if (v2Enabled && semanticSearchSettingsBuilder != null) {
      builders.add(semanticSearchSettingsBuilder);
    }

    if (builders.isEmpty()) {
      log.warn(
          "Neither v2 nor v3 entity index is enabled. SettingsBuilder will return empty settings.");
    }

    return new DelegatingSettingsBuilder(builders);
  }
}
