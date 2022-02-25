package com.linkedin.gms.factory.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import(EntityRegistryFactory.class)
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SettingsBuilderFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Value("${elasticsearch.index.mainTokenizer}")
  private String mainTokenizer;

  @Bean("settingsBuilder")
  protected SettingsBuilder getInstance() {
    // Filter to process URNs
    ImmutableList.Builder<String> stopWords = ImmutableList.<String>builder().add("urn").add("li");
    // Add all entity names to stop word list
    entityRegistry.getEntitySpecs().values().forEach(entitySpec -> stopWords.add(entitySpec.getName().toLowerCase()));
    return new SettingsBuilder(stopWords.build(), mainTokenizer);
  }
}
