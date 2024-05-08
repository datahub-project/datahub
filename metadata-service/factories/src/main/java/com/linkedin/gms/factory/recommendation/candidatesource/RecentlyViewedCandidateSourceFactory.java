package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RecentlyViewedCandidateSourceFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Bean(name = "recentlyViewedCandidateSource")
  @Nonnull
  protected RecentlyViewedSource getInstance() {
    return new RecentlyViewedSource(searchClient, indexConvention, entityService);
  }
}
