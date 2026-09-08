package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import com.linkedin.metadata.recommendation.candidatesource.UsageEventRecommendationBackend;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntityServiceFactory.class, UsageEventRecommendationBackendFactory.class})
public class RecentlyViewedCandidateSourceFactory {

  @Autowired
  @Qualifier("usageEventRecommendationBackend")
  private UsageEventRecommendationBackend usageEvents;

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Bean(name = "recentlyViewedCandidateSource")
  @Nonnull
  protected RecentlyViewedSource getInstance() {
    return new RecentlyViewedSource(usageEvents, entityService);
  }
}
