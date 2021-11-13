package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restoreaspect.RestoreAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class RestoreAspectConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "restoreAspect")
  @DependsOn({"entityService", "entityRegistry"})
  @Nonnull

  public RestoreAspect createInstance() {
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);
    return new RestoreAspect(entityService, entityRegistry);
  }
}
