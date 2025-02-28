package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restoreaspect.RestoreAspect;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestoreAspectConfig {

  @Bean(name = "restoreAspect")
  @Nonnull
  public RestoreAspect createInstance(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      final EntityService<?> entityService) {
    return new RestoreAspect(systemOperationContext, entityService);
  }
}
