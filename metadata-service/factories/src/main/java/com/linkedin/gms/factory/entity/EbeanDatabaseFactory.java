package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@Slf4j
public class EbeanDatabaseFactory {
  public static final String EBEAN_MODEL_PACKAGE = EbeanAspectV2.class.getPackage().getName();

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "ebeanServer")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected Database createServer(@Qualifier("gmsEbeanServiceConfig") DatabaseConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    // TODO: Consider supporting SCSI
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the server. Is it up?");
      throw ne;
    }
  }

  @Bean(name = "ebeanPrimaryServer")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected Database ebeanPrimaryServer(@Qualifier("gmsEbeanPrimaryServiceConfig") DatabaseConfig primaryServerConfig,
                                        @Qualifier("ebeanServer") Database fallbackServer) {
    if (primaryServerConfig != null) {
      // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
      if (!primaryServerConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
        primaryServerConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
      }
      // TODO: Consider supporting SCSI
      try {
        return io.ebean.DatabaseFactory.create(primaryServerConfig);
      } catch (NullPointerException ne) {
        log.error("Failed to connect to the server. Is it up?");
        throw ne;
      }
    }
    return fallbackServer;
  }
}
