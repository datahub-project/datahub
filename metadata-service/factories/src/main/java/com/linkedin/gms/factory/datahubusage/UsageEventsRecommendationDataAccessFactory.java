package com.linkedin.gms.factory.datahubusage;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationDataAccess;
import com.linkedin.metadata.datahubusage.opensearch.OpenSearchUsageEventsRecommendationDataAccess;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * OpenSearch fallback only. Postgres mode wires {@link
 * com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsRecommendationQueries} as the sole
 * {@link UsageEventsRecommendationDataAccess} implementation (no extra forwarding bean).
 */
@Configuration
@Import(IndexConventionFactory.class)
public class UsageEventsRecommendationDataAccessFactory {

  @Bean
  @ConditionalOnMissingBean(UsageEventsRecommendationDataAccess.class)
  @ConditionalOnBean(SearchClientShim.class)
  UsageEventsRecommendationDataAccess openSearchUsageEventsRecommendationDataAccess(
      SearchClientShim<?> searchClientShim,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return new OpenSearchUsageEventsRecommendationDataAccess(searchClientShim, indexConvention);
  }
}
