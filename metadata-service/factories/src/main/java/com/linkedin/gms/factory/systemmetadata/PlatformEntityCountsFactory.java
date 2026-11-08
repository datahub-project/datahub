package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.PlatformEntityCounts;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PlatformEntityCountsFactory {

  @Bean(name = "platformEntityCounts")
  @Nonnull
  protected PlatformEntityCounts platformEntityCounts(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      ConfigurationProvider configurationProvider) {
    int maxEntityTypes =
        configurationProvider.getCache().getEntityCounts().getKeyAspect().getMaxEntityTypes();
    return new PlatformEntityCounts(
        entityRegistry, configurationProvider.getElasticSearch().getEntityIndex(), maxEntityTypes);
  }
}
