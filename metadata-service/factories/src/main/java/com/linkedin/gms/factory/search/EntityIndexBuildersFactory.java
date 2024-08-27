package com.linkedin.gms.factory.search;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityIndexBuildersFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("settingsBuilder")
  private SettingsBuilder settingsBuilder;

  @Bean
  protected EntityIndexBuilders entityIndexBuilders() {
    return new EntityIndexBuilders(
        components.getIndexBuilder(),
        entityRegistry,
        components.getIndexConvention(),
        settingsBuilder);
  }
}
