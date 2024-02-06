package com.linkedin.gms.factory.entityclient;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class EntityClientConfigFactory {

  @Bean
  public EntityClientCacheConfig entityClientCacheConfig(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider) {
    return configurationProvider.getCache().getClient().getEntityClient();
  }
}
