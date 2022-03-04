package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.TopTermsSource;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import({EntitySearchServiceFactory.class, EntityServiceFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TopTermsCandidateSourceFactory {

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Value("${recommendationService.topTerms.offline}")
  private Boolean fetchOffline;

  @Bean(name = "topTermsCandidateSource")
  @Nonnull
  protected TopTermsSource getInstance() {
    return new TopTermsSource(entitySearchService, entityService, fetchOffline);
  }
}
