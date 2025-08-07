package com.linkedin.gms.factory.ingestion;

import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Import({SystemAuthenticationFactory.class})
public class IngestionSchedulerFactory {

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Value("${ingestion.scheduler.delayIntervalSeconds:45}") // Boot up ingestion source cache after
  // waiting 45 seconds for startup.
  private Integer delayIntervalSeconds;

  @Value("${ingestion.scheduler.refreshIntervalSeconds}") // By default, refresh ingestion
  // sources 2 times per day.
  private Integer refreshIntervalSeconds;

  @Bean(name = "ingestionScheduler")
  @Scope("singleton")
  @Nonnull
  protected IngestionScheduler getInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final SystemEntityClient entityClient) {
    return new IngestionScheduler(
        systemOpContext,
        entityClient,
        configProvider.getIngestion(),
        delayIntervalSeconds,
        refreshIntervalSeconds);
  }
}
