package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.LocalEbeanConfigFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
@ConditionalOnProperty(name = "datahub.readOnly", havingValue = "false", matchIfMissing = true)
public class EbeanReadPoolConfigFactory {

  @Autowired private LocalEbeanConfigFactory localEbeanConfigFactory;

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean("ebeanReadPoolDataSourceConfig")
  @ConditionalOnProperty(prefix = "ebean.readPool", name = "enabled", havingValue = "true")
  @Nonnull
  protected DataSourceConfig ebeanReadPoolDataSourceConfig(
      @Qualifier("ebeanDataSourceConfig") DataSourceConfig primaryConfig) {
    ReadPoolConfiguration readPool =
        Objects.requireNonNull(
            configurationProvider.getEbean().getReadPool(),
            "ebean.readPool must be configured in application.yaml");
    String readUrl =
        StringUtils.hasText(readPool.getUrl())
            ? readPool.getUrl()
            : configurationProvider.getEbean().getUrl();
    DataSourceConfig readConfig =
        localEbeanConfigFactory.buildDataSourceConfig(
            readUrl, null); // metric listener name set below
    if (readPool.getMinConnections() > 0) {
      readConfig.setMinConnections((int) readPool.getMinConnections());
    }
    if (readPool.getMaxConnections() > 0) {
      readConfig.setMaxConnections((int) readPool.getMaxConnections());
    }
    if (readPool.getMaxInactiveTimeSeconds() > 0) {
      readConfig.setMaxInactiveTimeSecs((int) readPool.getMaxInactiveTimeSeconds());
    }
    if (readPool.getMaxAgeMinutes() > 0) {
      readConfig.setMaxAgeMinutes((int) readPool.getMaxAgeMinutes());
    }
    if (readPool.getLeakTimeMinutes() > 0) {
      readConfig.setLeakTimeMinutes((int) readPool.getLeakTimeMinutes());
    }
    if (readPool.getWaitTimeoutMillis() > 0) {
      readConfig.setWaitTimeoutMillis((int) readPool.getWaitTimeoutMillis());
    }
    readConfig.setReadOnly(true);
    readConfig.setAutoCommit(true);
    boolean distinctEndpoint = !readUrl.equals(configurationProvider.getEbean().getUrl());
    if (distinctEndpoint) {
      log.info("Ebean READ pool configured for distinct endpoint: {}", readUrl);
    } else {
      log.info("Ebean READ pool configured as split-pool (same URL as PRIMARY)");
    }
    return readConfig;
  }

  @Bean("gmsEbeanReadPoolDatabaseConfig")
  @ConditionalOnProperty(prefix = "ebean.readPool", name = "enabled", havingValue = "true")
  @Nonnull
  protected DatabaseConfig gmsEbeanReadPoolDatabaseConfig(
      @Qualifier("ebeanReadPoolDataSourceConfig") DataSourceConfig readPoolDataSourceConfig) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gmsEbeanReadPoolDatabaseConfig");
    serverConfig.setDataSourceConfig(readPoolDataSourceConfig);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("ebeanReadPoolServer")
  @ConditionalOnProperty(prefix = "ebean.readPool", name = "enabled", havingValue = "true")
  @Nonnull
  protected Database ebeanReadPoolServer(
      @Qualifier("gmsEbeanReadPoolDatabaseConfig") DatabaseConfig serverConfig) {
    String modelPackage = EbeanAspectV2.class.getPackage().getName();
    if (!serverConfig.getPackages().contains(modelPackage)) {
      serverConfig.getPackages().add(modelPackage);
    }
    return DatabaseFactory.create(serverConfig);
  }
}
