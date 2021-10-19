package com.linkedin.gms.factory.recommendation;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.recommendation.RecommendationService;
import com.linkedin.metadata.recommendation.candidatesource.HighUsageCandidateSource;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedCandidateSource;
import com.linkedin.metadata.recommendation.candidatesource.TopPlatformsCandidateSource;
import com.linkedin.metadata.recommendation.ranker.SimpleRecommendationRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class RecommendationServiceFactory {

  @Autowired
  private TopPlatformsCandidateSource topPlatformsCandidateSource;

  @Autowired
  private RecentlyViewedCandidateSource recentlyViewedCandidateSource;

  @Autowired
  private HighUsageCandidateSource highUsageCandidateSource;

  @Bean
  @Nonnull
  protected RecommendationService getInstance() {
    return new RecommendationService(
        ImmutableList.of(topPlatformsCandidateSource, recentlyViewedCandidateSource, highUsageCandidateSource),
        new SimpleRecommendationRanker());
  }
}
