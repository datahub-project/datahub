package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.metadata.recommendation.candidatesource.RecentlySearchedSource;
import com.linkedin.metadata.recommendation.candidatesource.UsageEventRecommendationBackend;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({UsageEventRecommendationBackendFactory.class})
public class RecentlySearchedCandidateSourceFactory {

  @Autowired
  @Qualifier("usageEventRecommendationBackend")
  private UsageEventRecommendationBackend usageEvents;

  @Bean(name = "recentlySearchedCandidateSource")
  @Nonnull
  protected RecentlySearchedSource getInstance() {
    return new RecentlySearchedSource(usageEvents);
  }
}
