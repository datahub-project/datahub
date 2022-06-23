package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.TopPlatformsSource;
import com.linkedin.metadata.search.SearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({EntityServiceFactory.class, SearchServiceFactory.class})
public class TopPlatformsCandidateSourceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Autowired
  @Qualifier("searchService")
  private SearchService searchService;

  @Bean(name = "topPlatformsCandidateSource")
  @Nonnull
  protected TopPlatformsSource getInstance() {
    return new TopPlatformsSource(entityService, searchService);
  }
}
