package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class NoCodeUpgradeConfig {

  @Autowired ApplicationContext applicationContext;

  @Bean(name = "noCodeUpgrade")
  @DependsOn({"ebeanServer", "entityService", "systemRestliEntityClient", "entityRegistry"})
  @Nonnull
  public NoCodeUpgrade createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final SystemRestliEntityClient entityClient =
        applicationContext.getBean(SystemRestliEntityClient.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new NoCodeUpgrade(ebeanServer, entityService, entityRegistry, entityClient);
  }
}
