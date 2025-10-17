package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
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
import org.springframework.context.annotation.Import;

@Configuration
@Import(EntityRegistryFactory.class)
@Slf4j
public class SettingsBuilderFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean("legacySettingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v2.enabled", havingValue = "true")
  @Nonnull
  protected SettingsBuilder createLegacySettingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    IndexConfiguration indexConfig = configProvider.getElasticSearch().getIndex();
    log.info("Creating LegacySettingsBuilder bean");
    return new V2LegacySettingsBuilder(indexConfig, indexConvention);
  }

  @Bean("multiEntitySettingsBuilder")
  @ConditionalOnProperty(name = "elasticsearch.entityIndex.v3.enabled", havingValue = "true")
  @Nonnull
  protected SettingsBuilder createMultiEntitySettingsBuilder(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    EntityIndexConfiguration entityIndexConfig = configProvider.getElasticSearch().getEntityIndex();
    log.info("Creating MultiEntitySettingsBuilder bean");
    try {
      return new MultiEntitySettingsBuilder(entityIndexConfig, indexConvention);
    } catch (IOException e) {
      log.error("Failed to initialize MultiEntitySettingsBuilder", e);
      throw new RuntimeException("Failed to initialize MultiEntitySettingsBuilder", e);
    }
  }

  @Bean("settingsBuilder")
  protected SettingsBuilder getInstance(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      @Qualifier("legacySettingsBuilder") @Nullable SettingsBuilder legacySettingsBuilder,
      @Qualifier("multiEntitySettingsBuilder") @Nullable
          SettingsBuilder multiEntitySettingsBuilder) {

    List<SettingsBuilder> builders = new ArrayList<>();

    if (legacySettingsBuilder != null) {
      builders.add(legacySettingsBuilder);
    }

    if (multiEntitySettingsBuilder != null) {
      builders.add(multiEntitySettingsBuilder);
    }

    if (builders.isEmpty()) {
      log.warn(
          "Neither v2 nor v3 entity index is enabled. SettingsBuilder will return empty settings.");
    }

    return new DelegatingSettingsBuilder(builders);
  }
}
