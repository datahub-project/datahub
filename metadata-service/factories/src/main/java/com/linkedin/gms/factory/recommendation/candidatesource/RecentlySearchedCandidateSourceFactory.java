package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.datahubusage.RecentSearchRecommendationAccessFactory;
import com.linkedin.metadata.datahubusage.RecentSearchRecommendationAccess;
import com.linkedin.metadata.recommendation.candidatesource.RecentlySearchedSource;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(RecentSearchRecommendationAccessFactory.class)
public class RecentlySearchedCandidateSourceFactory {

  @Autowired
  private ObjectProvider<RecentSearchRecommendationAccess> recentSearchRecommendationAccessProvider;

  @Bean(name = "recentlySearchedCandidateSource")
  @Nonnull
  protected RecentlySearchedSource getInstance() {
    return new RecentlySearchedSource(recentSearchRecommendationAccessProvider.getIfAvailable());
  }
}
