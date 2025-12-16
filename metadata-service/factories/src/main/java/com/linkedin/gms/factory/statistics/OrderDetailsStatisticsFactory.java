package com.linkedin.gms.factory.statistics;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Factory to create and register OrderDetailsStatistics beans.
 *
 * <p>Only creates beans when datahub.freeTrialInstance=true.
 */
@Slf4j
@Configuration
public class OrderDetailsStatisticsFactory {

  @Value("${datahub.freeTrialInstance:false}")
  private Boolean freeTrialInstance;

  @Bean
  @ConditionalOnProperty(
      name = "datahub.freeTrialInstance",
      havingValue = "true",
      matchIfMissing = false)
  public OrderDetailsStatisticsGenerator orderDetailsStatisticsGenerator(
      EntityService<?> entityService,
      @Qualifier("timeseriesAspectService") TimeseriesAspectService timeseriesAspectService) {
    log.info(
        "Creating OrderDetailsStatisticsGenerator bean (freeTrialInstance={})", freeTrialInstance);
    return new OrderDetailsStatisticsGenerator(entityService, timeseriesAspectService);
  }
}
