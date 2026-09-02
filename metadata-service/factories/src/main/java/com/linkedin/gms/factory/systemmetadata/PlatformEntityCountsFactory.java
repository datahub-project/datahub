package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.PlatformEntityCounts;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PlatformEntityCountsFactory {

  @Bean(name = "platformEntityCounts")
  @Nonnull
  protected PlatformEntityCounts platformEntityCounts(
      @Qualifier("searchClientShim") SearchClientShim<?> searchClient,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      ConfigurationProvider configurationProvider) {
    int maxEntityTypes =
        configurationProvider.getCache().getEntityCounts().getKeyAspect().getMaxEntityTypes();
    return new PlatformEntityCounts(
        searchClient,
        entityRegistry,
        configurationProvider.getElasticSearch().getEntityIndex(),
        maxEntityTypes);
  }
}
