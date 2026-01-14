package com.linkedin.gms.factory.settings;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.statistics.OrderDetailsStatisticsGenerator;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.SampleDataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory to create SampleDataService bean.
 *
 * <p>Only creates the bean when datahub.isFreeTrialInstance=true.
 */
@Slf4j
@Configuration
public class SampleDataServiceFactory {

  @Bean(name = "sampleDataService")
  @ConditionalOnProperty(
      name = "datahub.freeTrialInstance",
      havingValue = "true",
      matchIfMissing = false)
  @Nonnull
  protected SampleDataService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("entitySearchService") final EntitySearchService searchService,
      @Qualifier("timeseriesAspectService") final TimeseriesAspectService timeseriesAspectService,
      @Nullable final OrderDetailsStatisticsGenerator statisticsGenerator) {
    log.info(
        "Creating SampleDataService bean for free trial instance (statisticsGenerator={})",
        statisticsGenerator != null ? "present" : "null");
    return new SampleDataService(
        entityClient, searchService, timeseriesAspectService, statisticsGenerator);
  }
}
