package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import static com.linkedin.metadata.entity.ebean.EbeanAspectDao.EBEAN_MODEL_PACKAGE;


@Configuration
public class NoCodeUpgradeConfig {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "noCodeUpgrade")
  @DependsOn({"gmsEbeanServiceConfig", "entityService", "entityClient"})
  @Nonnull
  public NoCodeUpgrade createInstance() {
    final ServerConfig serverConfig = applicationContext.getBean(ServerConfig.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final EntityClient entityClient = applicationContext.getBean(EntityClient.class);
    final SnapshotEntityRegistry entityRegistry = new SnapshotEntityRegistry();

    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }

    return new NoCodeUpgrade(EbeanServerFactory.create(serverConfig), entityService, entityRegistry, entityClient);
  }
}
