package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EntityRegistryFactory.class)
public class SettingsBuilderFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Value("${elasticsearch.index.mainTokenizer}")
  private String mainTokenizer;

  @Bean("settingsBuilder")
  protected SettingsBuilder getInstance() {
    return new SettingsBuilder(mainTokenizer);
  }
}
