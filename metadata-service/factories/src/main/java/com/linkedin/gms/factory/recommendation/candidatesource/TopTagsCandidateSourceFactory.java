package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.candidatesource.TopTagsSource;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({EntitySearchServiceFactory.class, EntityRegistryFactory.class})
public class TopTagsCandidateSourceFactory {

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  private CacheManager cacheManager;

  @Bean(name = "topTagsCandidateSource")
  @Nonnull
  protected TopTagsSource getInstance() {
    return new TopTagsSource(entitySearchService, entityRegistry, cacheManager);
  }
}
