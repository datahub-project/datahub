package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RollbackServiceFactory {

  @Bean
  @Nonnull
  protected RollbackService rollbackService(
      final EntityService<?> entityService,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService) {
    return new RollbackService(entityService, systemMetadataService, timeseriesAspectService);
  }
}
