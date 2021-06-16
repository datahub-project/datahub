package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.nocode.CreateAspectIndexUpgrade;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import static com.linkedin.metadata.entity.ebean.EbeanAspectDao.*;


@Configuration
public class CreateAspectIndexUpgradeConfig {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "createAspectIndexUpgrade")
  @DependsOn({"gmsEbeanServiceConfig"})
  @Nonnull
  public CreateAspectIndexUpgrade createInstance() {
    final ServerConfig serverConfig = applicationContext.getBean(ServerConfig.class);
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    return new CreateAspectIndexUpgrade(EbeanServerFactory.create(serverConfig));
  }
}
