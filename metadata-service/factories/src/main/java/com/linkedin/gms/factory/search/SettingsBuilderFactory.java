package com.linkedin.gms.factory.search;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EntityRegistryFactory.class)
public class SettingsBuilderFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean("settingsBuilder")
  protected SettingsBuilder getInstance(
      ConfigurationProvider configProvider,
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return new DelegatingSettingsBuilder(
        configProvider.getElasticSearch().getEntityIndex(),
        configProvider.getElasticSearch().getIndex(),
        indexConvention);
  }
}
