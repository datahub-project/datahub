package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.EbeanServer;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Slf4j
@Configuration
public class EbeanServerFactory {
  public static final String EBEAN_MODEL_PACKAGE = EbeanAspectV2.class.getPackage().getName();

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "ebeanServer")
  @DependsOn({"gmsEbeanServiceConfig"})
  @Nonnull
  protected EbeanServer createServer() {
    try {
      ServerConfig serverConfig = applicationContext.getBean(ServerConfig.class);

      // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
      if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
        serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
      }

      io.ebean.EbeanServer factory = io.ebean.EbeanServerFactory.create(serverConfig);

      if (factory == null) {
        throw new RuntimeException("Failed to create ebean server, EbeanServerFactory is null!");
      }

      // TODO: Consider supporting SCSI
      return factory;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create ebean server!", e);
    }
  }
}
