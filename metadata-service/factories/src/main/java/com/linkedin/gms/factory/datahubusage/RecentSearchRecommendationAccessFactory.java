package com.linkedin.gms.factory.datahubusage;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.datahubusage.RecentSearchRecommendationAccess;
import com.linkedin.metadata.datahubusage.opensearch.OpenSearchRecentSearchRecommendationAccess;
import com.linkedin.metadata.datahubusage.postgres.PostgresRecentSearchRecommendationAccess;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(IndexConventionFactory.class)
public class RecentSearchRecommendationAccessFactory {

  @Bean
  @ConditionalOnBean(PostgresUsageEventsStore.class)
  RecentSearchRecommendationAccess postgresRecentSearchRecommendationAccess(
      PostgresUsageEventsStore postgresUsageEventsStore,
      ConfigurationProvider configurationProvider) {
    return new PostgresRecentSearchRecommendationAccess(
        postgresUsageEventsStore,
        configurationProvider
            .getPlatformAnalytics()
            .getUsageEvents()
            .getRecommendationLookbackDays());
  }

  @Bean
  @ConditionalOnMissingBean(RecentSearchRecommendationAccess.class)
  @ConditionalOnBean(SearchClientShim.class)
  RecentSearchRecommendationAccess openSearchRecentSearchRecommendationAccess(
      SearchClientShim<?> searchClientShim,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return new OpenSearchRecentSearchRecommendationAccess(searchClientShim, indexConvention);
  }
}
