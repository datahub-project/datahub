package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.metadata.recommendation.candidatesource.DomainsCandidateSource;
import com.linkedin.metadata.search.SearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SearchServiceFactory.class})
public class DomainsCandidateSourceFactory {

  @Autowired
  @Qualifier("searchService")
  private SearchService searchService;

  @Bean(name = "domainsCandidateSource")
  @Nonnull
  protected DomainsCandidateSource getInstance() {
    return new DomainsCandidateSource(searchService);
  }
}
