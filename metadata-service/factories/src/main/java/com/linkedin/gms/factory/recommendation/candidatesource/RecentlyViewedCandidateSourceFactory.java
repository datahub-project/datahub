package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.datahubusage.UsageEventsRecommendationDataAccessFactory;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationDataAccess;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntityServiceFactory.class, UsageEventsRecommendationDataAccessFactory.class})
public class RecentlyViewedCandidateSourceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Autowired
  private ObjectProvider<UsageEventsRecommendationDataAccess>
      usageEventsRecommendationDataAccessProvider;

  @Bean(name = "recentlyViewedCandidateSource")
  @Nonnull
  protected RecentlyViewedSource recentlyViewedCandidateSource() {
    return new RecentlyViewedSource(
        usageEventsRecommendationDataAccessProvider.getIfAvailable(), entityService);
  }
}
