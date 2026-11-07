package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.DocumentIdSchemeStampingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SearchEngineStructuredPropertyMappingLookup;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2SemanticSearchMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.DocumentV3EmbeddingMappingContributor;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3MappingContributor;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(V3DocumentIdSchemeFactory.class)
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
  @ConditionalOnExpression(
      "${elasticsearch.entityIndex.v2.enabled:false} or ${elasticsearch.entityIndex.v3.enabled:false}")
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
      @Autowired(required = false) @Nullable List<V3MappingContributor> mappingContributors,
      @Qualifier("v3DocumentIdScheme") V3DocumentIdScheme v3DocumentIdScheme) {
    EntityIndexConfiguration entityIndexConfig =
        searchClusterRegistry.configFor(SearchComponent.SEARCH_V3).getEntityIndex();
    int keywordMaxLength = resolveKeywordMaxLength(configProvider);
    SearchClientShim<?> v3Client = searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3);
    SemanticSearchConfiguration semanticConfig = entityIndexConfig.getSemanticSearch();
    List<V3MappingContributor> contributors =
        new ArrayList<>(mappingContributors == null ? List.of() : mappingContributors);
    contributors.add(new DocumentV3EmbeddingMappingContributor(semanticConfig, v3Client));
    log.info(
        "Creating MultiEntityMappingsBuilder bean (engineType={}; document V3 embeddings when semanticSearch is on)",
        v3Client.getEngineType());
    try {
      // V3 documents carry hashed ids. Recording the running scheme on every V3 index at creation
      // lets the incremental upgrade detect a later hasher change and rebuild the index.
      return new DocumentIdSchemeStampingMappingsBuilder(
          new MultiEntityMappingsBuilder(
              entityIndexConfig, v3Client, keywordMaxLength, contributors),
          v3DocumentIdScheme);
    } catch (IOException e) {
      log.error("Failed to initialize MultiEntityMappingsBuilder", e);
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
  }

  // Created when a hold might turn V2 on. Assembled below only while the live V2 flag is on.
  @Bean("semanticSearchMappingsBuilder")
  @ConditionalOnExpression(
      "${elasticsearch.entityIndex.semanticSearch.enabled:false} and"
          + " (${elasticsearch.entityIndex.v2.enabled:false} or"
          + " ${elasticsearch.entityIndex.v3.enabled:false})")
  @Nonnull
  protected MappingsBuilder createSemanticSearchMappingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      SearchClusterRegistry searchClusterRegistry) {
    EntityIndexConfiguration semanticEntityIndex =
        searchClusterRegistry.configFor(SearchComponent.SEMANTIC).getEntityIndex();
    SemanticSearchConfiguration semanticConfig = semanticEntityIndex.getSemanticSearch();

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
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Qualifier("legacyMappingsBuilder") @Nullable MappingsBuilder legacyMappingsBuilder,
      @Qualifier("multiEntityMappingsBuilder") @Nullable MappingsBuilder multiEntityMappingsBuilder,
      @Qualifier("semanticSearchMappingsBuilder") @Nullable
          MappingsBuilder semanticSearchMappingsBuilder) {
    List<MappingsBuilder> builders = new ArrayList<>();
    // The beans exist whenever V3 is on, so a hold can write V2. Index builds include them only
    // while that live flag is on, so a fresh V3 install does not create V2 indices.
    boolean v2Enabled =
        elasticSearchConfiguration.getEntityIndex() != null
            && elasticSearchConfiguration.getEntityIndex().getV2() != null
            && elasticSearchConfiguration.getEntityIndex().getV2().isEnabled();

    if (v2Enabled && legacyMappingsBuilder != null) {
      builders.add(legacyMappingsBuilder);
    }

    if (multiEntityMappingsBuilder != null) {
      builders.add(multiEntityMappingsBuilder);
    }

    if (v2Enabled && semanticSearchMappingsBuilder != null) {
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
