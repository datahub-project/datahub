package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CassandraConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import com.linkedin.metadata.entity.storage.PrimaryStorageRegistry;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
public class PrimaryStorageResolverFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired(required = false)
  @Nullable
  private MetricUtils metricUtils;

  @Bean
  @Nonnull
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  protected PrimaryStorageResolver ebeanPrimaryStorageResolver(
      @Qualifier("ebeanServer") Database primaryServer,
      @Autowired(required = false) @Qualifier("ebeanReadPoolServer")
          ObjectProvider<Database> readPoolServer) {
    return buildResolver(
        primaryServer,
        readPoolServer,
        configurationProvider.getEbean(),
        configurationProvider.getDatahub().isReadOnly(),
        "ebean");
  }

  @Bean
  @Nonnull
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  protected PrimaryStorageResolver cassandraPrimaryStorageResolver(
      @Qualifier("cassandraSession") CqlSession primarySession,
      @Autowired(required = false) @Qualifier("cassandraReadPoolSession")
          ObjectProvider<CqlSession> readPoolSession) {
    CassandraConfiguration cassandra = configurationProvider.getCassandra();
    boolean readOnly = configurationProvider.getDatahub().isReadOnly();
    boolean distinctReadEndpoint = false;
    if (cassandra != null
        && cassandra.getReadPool() != null
        && cassandra.getReadPool().isEnabled()
        && !readOnly) {
      ReadPoolConfiguration readPool = cassandra.getReadPool();
      String readHosts =
          StringUtils.hasText(readPool.getHosts()) ? readPool.getHosts() : cassandra.getHosts();
      distinctReadEndpoint = !readHosts.equals(cassandra.getHosts());
    }
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(distinctReadEndpoint);
    registry.register(StorageTarget.PRIMARY, primarySession);
    if (cassandra != null
        && cassandra.getReadPool() != null
        && cassandra.getReadPool().isEnabled()
        && !readOnly
        && readPoolSession.getIfAvailable() != null) {
      registry.register(StorageTarget.READ, readPoolSession.getObject());
    } else if (cassandra != null
        && cassandra.getReadPool() != null
        && cassandra.getReadPool().isEnabled()
        && readOnly) {
      log.warn(
          "cassandra.readPool.enabled is true but datahub.readOnly=true; READ pool will not be registered");
    }
    ReadPreference deployDefault =
        registry.has(StorageTarget.READ) ? ReadPreference.READ : ReadPreference.PRIMARY;
    return new PrimaryStorageResolver(registry, deployDefault, metricUtils, "cassandra");
  }

  private PrimaryStorageResolver buildResolver(
      @Nonnull Database primaryServer,
      @Nonnull ObjectProvider<Database> readPoolServer,
      @Nonnull EbeanConfiguration ebeanConfiguration,
      boolean readOnly,
      @Nonnull String storeTag) {
    boolean distinctReadEndpoint = false;
    ReadPoolConfiguration readPool = ebeanConfiguration.getReadPool();
    if (readPool != null && readPool.isEnabled() && !readOnly) {
      String readUrl =
          StringUtils.hasText(readPool.getUrl()) ? readPool.getUrl() : ebeanConfiguration.getUrl();
      distinctReadEndpoint = !readUrl.equals(ebeanConfiguration.getUrl());
    }
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(distinctReadEndpoint);
    registry.register(StorageTarget.PRIMARY, primaryServer);
    if (readPool != null
        && readPool.isEnabled()
        && !readOnly
        && readPoolServer.getIfAvailable() != null) {
      registry.register(StorageTarget.READ, readPoolServer.getObject());
    } else if (readPool != null && readPool.isEnabled() && readOnly) {
      log.warn(
          "ebean.readPool.enabled is true but datahub.readOnly=true; READ pool will not be registered");
    }
    if (readPool != null && readPool.isEnabled() && readPoolServer.getIfAvailable() != null) {
      if (distinctReadEndpoint) {
        log.info("Ebean primary storage: READ pool registered (distinct replica endpoint)");
      } else {
        log.info("Ebean primary storage: READ pool registered (split-pool, same URL as PRIMARY)");
      }
    }
    ReadPreference deployDefault =
        registry.has(StorageTarget.READ) ? ReadPreference.READ : ReadPreference.PRIMARY;
    return new PrimaryStorageResolver(registry, deployDefault, metricUtils, storeTag);
  }
}
