package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.candidatesource.TopTermsSource;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntitySearchServiceFactory.class, EntityServiceFactory.class})
public class TopTermsCandidateSourceFactory {
  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Bean(name = "topTermsCandidateSource")
  @Nonnull
  protected TopTermsSource getInstance(
      final EntityService<?> entityService, final EntityRegistry entityRegistry) {
    return new TopTermsSource(entitySearchService, entityService, entityRegistry);
  }
}
