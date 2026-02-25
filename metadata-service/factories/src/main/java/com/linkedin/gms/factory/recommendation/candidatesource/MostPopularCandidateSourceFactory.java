package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.MostPopularSource;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({IndexConventionFactory.class, EntityServiceFactory.class})
public class MostPopularCandidateSourceFactory {
  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Bean(name = "mostPopularCandidateSource")
  @Nonnull
  protected MostPopularSource getInstance() {
    return new MostPopularSource(searchClient, indexConvention, entityService);
  }
}
