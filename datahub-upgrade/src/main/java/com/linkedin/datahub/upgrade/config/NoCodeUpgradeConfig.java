package com.linkedin.datahub.upgrade.config;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
  @DependsOn({"ebeanServer", "entityService", "systemAuthentication", "restliEntityClient", "entityRegistry"})
  @Nonnull
  public NoCodeUpgrade createInstance() {
    final EbeanServer ebeanServer = applicationContext.getBean(EbeanServer.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final Authentication systemAuthentication = applicationContext.getBean(Authentication.class);
    final RestliEntityClient entityClient = applicationContext.getBean(RestliEntityClient.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new NoCodeUpgrade(ebeanServer, entityService, entityRegistry, systemAuthentication, entityClient);
  }
}
