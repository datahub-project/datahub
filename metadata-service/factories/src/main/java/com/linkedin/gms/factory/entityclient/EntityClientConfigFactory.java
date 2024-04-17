package com.linkedin.gms.factory.entityclient;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityClientConfigFactory {

  @Bean
  public EntityClientCacheConfig entityClientCacheConfig(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider) {
    return configurationProvider.getCache().getClient().getEntityClient();
  }
}
