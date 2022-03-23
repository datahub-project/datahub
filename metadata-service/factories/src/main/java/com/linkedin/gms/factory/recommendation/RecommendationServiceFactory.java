package com.linkedin.gms.factory.recommendation;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.recommendation.candidatesource.DomainsCandidateSourceFactory;
import com.linkedin.gms.factory.recommendation.candidatesource.MostPopularCandidateSourceFactory;
import com.linkedin.gms.factory.recommendation.candidatesource.RecentlyViewedCandidateSourceFactory;
import com.linkedin.gms.factory.recommendation.candidatesource.TopPlatformsCandidateSourceFactory;
import com.linkedin.gms.factory.recommendation.candidatesource.TopTagsCandidateSourceFactory;
import com.linkedin.gms.factory.recommendation.candidatesource.TopTermsCandidateSourceFactory;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.recommendation.candidatesource.DomainsCandidateSource;
import com.linkedin.metadata.recommendation.candidatesource.MostPopularSource;
import com.linkedin.metadata.recommendation.candidatesource.RecentlySearchedSource;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import com.linkedin.metadata.recommendation.candidatesource.RecommendationSource;
import com.linkedin.metadata.recommendation.candidatesource.TopPlatformsSource;
import com.linkedin.metadata.recommendation.candidatesource.TopTagsSource;
import com.linkedin.metadata.recommendation.candidatesource.TopTermsSource;
import com.linkedin.metadata.recommendation.ranker.SimpleRecommendationRanker;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({TopPlatformsCandidateSourceFactory.class, RecentlyViewedCandidateSourceFactory.class,
    MostPopularCandidateSourceFactory.class, TopTagsCandidateSourceFactory.class, TopTermsCandidateSourceFactory.class, DomainsCandidateSourceFactory.class})
public class RecommendationServiceFactory {

  @Autowired
  @Qualifier("topPlatformsCandidateSource")
  private TopPlatformsSource topPlatformsCandidateSource;

  @Autowired
  @Qualifier("recentlyViewedCandidateSource")
  private RecentlyViewedSource recentlyViewedCandidateSource;

  @Autowired
  @Qualifier("mostPopularCandidateSource")
  private MostPopularSource _mostPopularCandidateSource;

  @Autowired
  @Qualifier("topTagsCandidateSource")
  private TopTagsSource topTagsCandidateSource;

  @Autowired
  @Qualifier("topTermsCandidateSource")
  private TopTermsSource topTermsCandidateSource;

  @Autowired
  @Qualifier("domainsCandidateSource")
  private DomainsCandidateSource domainsCandidateSource;

  @Autowired
  @Qualifier("recentlySearchedCandidateSource")
  private RecentlySearchedSource recentlySearchedCandidateSource;

  @Bean
  @Nonnull
  protected RecommendationsService getInstance() {
    // TODO: Make this class-name pluggable to minimize merge conflict potential.
    // This is where you can add new recommendation modules.
    final List<RecommendationSource> candidateSources = ImmutableList.of(
        topPlatformsCandidateSource,
        domainsCandidateSource,
        recentlyViewedCandidateSource, _mostPopularCandidateSource,
        topTagsCandidateSource, topTermsCandidateSource, recentlySearchedCandidateSource);
    return new RecommendationsService(candidateSources, new SimpleRecommendationRanker());
  }
}
