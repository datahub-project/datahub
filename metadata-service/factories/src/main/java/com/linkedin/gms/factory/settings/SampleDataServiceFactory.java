package com.linkedin.gms.factory.settings;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.SampleDataService;
import javax.annotation.Nonnull;
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
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient) {
    log.info("Creating SampleDataService bean for free trial instance");
    return new SampleDataService(entityClient);
  }
}
