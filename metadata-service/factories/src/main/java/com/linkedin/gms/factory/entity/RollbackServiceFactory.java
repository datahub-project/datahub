package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.IngestionRollbackDispatcher;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RollbackServiceFactory {

  @Bean
  @Nonnull
  protected RollbackService rollbackService(
      final EntityService<?> entityService,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final ConfigurationProvider configurationProvider,
      final ObjectProvider<IngestionRollbackDispatcher> dispatcher) {
    final IngestionRollbackDispatcher resolvedDispatcher = dispatcher.getIfAvailable();
    if (resolvedDispatcher != null) {
      log.info(
          "Ingestion rollbacks are offered to {} first, else run in-process",
          resolvedDispatcher.getClass().getSimpleName());
    } else {
      log.info("No IngestionRollbackDispatcher bean available; ingestion rollbacks run in-process");
    }
    return new RollbackService(
        entityService,
        systemMetadataService,
        timeseriesAspectService,
        configurationProvider.getSystemMetadataService(),
        resolvedDispatcher);
  }
}
