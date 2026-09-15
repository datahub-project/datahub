package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SearchEngineStructuredPropertyMappingLookup;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2SemanticSearchMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3MappingContributor;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertyMappingLookup;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class MappingsBuilderFactory {

  @Bean
  @Nonnull
  protected StructuredPropertyMappingLookup structuredPropertyMappingLookup(
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry) {
    return new SearchEngineStructuredPropertyMappingLookup(indexConvention, searchClusterRegistry);
  }

  @Bean("legacyMappingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected MappingsBuilder createLegacyMappingsBuilder(
      ConfigurationProvider configProvider, SearchClusterRegistry searchClusterRegistry) {
    EntityIndexConfiguration entityIndexConfig =
        searchClusterRegistry.configFor(SearchComponent.SEARCH_V2).getEntityIndex();
    int keywordMaxLength = resolveKeywordMaxLength(configProvider);
    SearchClientShim<?> v2Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2);
    log.info(
        "Creating LegacyMappingsBuilder bean (engineType={} is diagnostic only; V2 mappings are engine-agnostic)",
        v2Client.getEngineType());
    return new V2MappingsBuilder(
        entityIndexConfig, v2Client.partialNgramConfig(), keywordMaxLength);
  }

  @Bean("multiEntityMappingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected MappingsBuilder createMultiEntityMappingsBuilder(
      ConfigurationProvider configProvider,
      SearchClusterRegistry searchClusterRegistry,
      @Autowired(required = false) @Nullable List<V3MappingContributor> mappingContributors) {
    EntityIndexConfiguration entityIndexConfig =
        searchClusterRegistry.configFor(SearchComponent.SEARCH_V3).getEntityIndex();
    int keywordMaxLength = resolveKeywordMaxLength(configProvider);
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    log.info(
        "Creating MultiEntityMappingsBuilder bean (engineType={} is diagnostic only; V3 mappings are engine-agnostic)",
        v3Client.getEngineType());
    try {
      return new MultiEntityMappingsBuilder(
          entityIndexConfig,
          keywordMaxLength,
          mappingContributors == null ? List.of() : mappingContributors);
    } catch (IOException e) {
      log.error("Failed to initialize MultiEntityMappingsBuilder", e);
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
  }

  @Bean("semanticSearchMappingsBuilder")
  @ConditionalOnProperty(
      name = "elasticsearch.entityIndex.semanticSearch.enabled",
      havingValue = "true")
  @Nonnull
  protected MappingsBuilder createSemanticSearchMappingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier("legacyMappingsBuilder") @Nullable MappingsBuilder v2MappingsBuilder,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry) {
    EntityIndexConfiguration semanticEntityIndex =
        searchClusterRegistry.configFor(SearchComponent.SEMANTIC).getEntityIndex();
    SemanticSearchConfiguration semanticConfig = semanticEntityIndex.getSemanticSearch();

    if (v2MappingsBuilder == null) {
      throw new IllegalStateException(
          "Semantic search requires v2 entity index to be enabled. "
              + "Please set elasticsearch.entityIndex.v2.enabled=true");
    }

    SearchClientShim<?> semanticClient = searchClusterRegistry.clientFor(SearchComponent.SEMANTIC);
    MappingsBuilder semanticMappingsBase =
        new V2MappingsBuilder(
            semanticEntityIndex,
            semanticClient.partialNgramConfig(),
            resolveKeywordMaxLength(configProvider));
    log.info(
        "Creating SemanticSearchMappingsBuilder bean for entities: {} engine: {}",
        semanticConfig.getEnabledEntities(),
        semanticClient.getEngineType());
    return new V2SemanticSearchMappingsBuilder(
        semanticMappingsBase, semanticConfig, indexConvention, semanticClient);
  }

  @Bean("mappingsBuilder")
  protected MappingsBuilder getInstance(
      @Qualifier("legacyMappingsBuilder") @Nullable MappingsBuilder legacyMappingsBuilder,
      @Qualifier("multiEntityMappingsBuilder") @Nullable MappingsBuilder multiEntityMappingsBuilder,
      @Qualifier("semanticSearchMappingsBuilder") @Nullable
          MappingsBuilder semanticSearchMappingsBuilder) {
    List<MappingsBuilder> builders = new ArrayList<>();

    if (legacyMappingsBuilder != null) {
      builders.add(legacyMappingsBuilder);
    }

    if (multiEntityMappingsBuilder != null) {
      builders.add(multiEntityMappingsBuilder);
    }

    if (semanticSearchMappingsBuilder != null) {
      builders.add(semanticSearchMappingsBuilder);
    }

    if (builders.isEmpty()) {
      log.warn("Neither v2 nor v3 entity index is enabled. Using NoOpMappingsBuilder.");
      builders.add(new NoOpMappingsBuilder());
    }

    return new DelegatingMappingsBuilder(builders);
  }

  private static int resolveKeywordMaxLength(@Nonnull ConfigurationProvider configProvider) {
    return configProvider.getStructuredProperties().getKeywordMaxLength();
  }
}
