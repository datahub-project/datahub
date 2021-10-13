package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;

import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import io.ebean.EbeanServer;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class NoCodeUpgradeConfig {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "noCodeUpgrade")
  @DependsOn({"ebeanServer", "entityService", "restliEntityClient"})
  @Nonnull
  public NoCodeUpgrade createInstance() {
    final EbeanServer ebeanServer = applicationContext.getBean(EbeanServer.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final RestliEntityClient entityClient = applicationContext.getBean(RestliEntityClient.class);
    final SnapshotEntityRegistry entityRegistry = new SnapshotEntityRegistry();

    return new NoCodeUpgrade(ebeanServer, entityService, entityRegistry, entityClient);
  }
}
