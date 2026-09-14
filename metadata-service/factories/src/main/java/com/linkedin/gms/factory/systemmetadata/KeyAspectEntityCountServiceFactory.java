package com.linkedin.gms.factory.systemmetadata;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCache;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KeyAspectEntityCountServiceFactory {

  @Bean(name = "keyAspectEntityCountService")
  @Nonnull
  protected KeyAspectEntityCountService getInstance(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("elasticSearchSystemMetadataService") SystemMetadataService systemMetadataService,
      ObjectProvider<HazelcastInstance> hazelcastInstance,
      ConfigurationProvider configurationProvider) {
    var keyAspectConfig = configurationProvider.getCache().getEntityCounts().getKeyAspect();
    KeyAspectEntityCountCache countCache =
        new KeyAspectEntityCountCache(keyAspectConfig, hazelcastInstance.getIfAvailable());
    return new KeyAspectEntityCountService(
        entityRegistry, systemMetadataService, countCache, keyAspectConfig.getMaxEntityTypes());
  }
}
