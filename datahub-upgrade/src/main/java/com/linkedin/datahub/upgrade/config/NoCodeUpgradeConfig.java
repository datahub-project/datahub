package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class NoCodeUpgradeConfig {

  @Autowired ApplicationContext applicationContext;

  @Bean(name = "noCodeUpgrade")
  @DependsOn({"ebeanServer", "entityService", "systemEntityClient", "entityRegistry"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public NoCodeUpgrade createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService<?> entityService = applicationContext.getBean(EntityService.class);
    final SystemEntityClient entityClient = applicationContext.getBean(SystemEntityClient.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new NoCodeUpgrade(ebeanServer, entityService, entityRegistry, entityClient);
  }

  @Bean(name = "noCodeUpgrade")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public NoCodeUpgrade createNotImplInstance() {
    log.warn("NoCode is not supported for cassandra!");
    return new NoCodeUpgrade(null, null, null, null);
  }
}
