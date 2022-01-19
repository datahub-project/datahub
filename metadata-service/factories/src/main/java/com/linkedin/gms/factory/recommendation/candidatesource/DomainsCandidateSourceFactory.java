package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.recommendation.candidatesource.DomainsCandidateSource;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntitySearchServiceFactory.class})
public class DomainsCandidateSourceFactory {

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Bean(name = "domainsCandidateSource")
  @Nonnull
  protected DomainsCandidateSource getInstance() {
    return new DomainsCandidateSource(entitySearchService);
  }
}
