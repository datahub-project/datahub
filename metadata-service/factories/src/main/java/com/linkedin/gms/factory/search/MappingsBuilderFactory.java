package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2SemanticSearchMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class MappingsBuilderFactory {

  @Bean("legacyMappingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected MappingsBuilder createLegacyMappingsBuilder(ConfigurationProvider configProvider) {
    EntityIndexConfiguration entityIndexConfig = configProvider.getElasticSearch().getEntityIndex();
    log.info("Creating LegacyMappingsBuilder bean");
    return new V2MappingsBuilder(entityIndexConfig);
  }

  @Bean("multiEntityMappingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected MappingsBuilder createMultiEntityMappingsBuilder(ConfigurationProvider configProvider) {
    EntityIndexConfiguration entityIndexConfig = configProvider.getElasticSearch().getEntityIndex();
    log.info("Creating MultiEntityMappingsBuilder bean");
    try {
      return new MultiEntityMappingsBuilder(entityIndexConfig);
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
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    SemanticSearchConfiguration semanticConfig =
        configProvider.getElasticSearch().getEntityIndex().getSemanticSearch();

    if (v2MappingsBuilder == null) {
      throw new IllegalStateException(
          "Semantic search requires v2 entity index to be enabled. "
              + "Please set elasticsearch.entityIndex.v2.enabled=true");
    }

    log.info(
        "Creating SemanticSearchMappingsBuilder bean for entities: {}",
        semanticConfig.getEnabledEntities());
    return new V2SemanticSearchMappingsBuilder(v2MappingsBuilder, semanticConfig, indexConvention);
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
}
