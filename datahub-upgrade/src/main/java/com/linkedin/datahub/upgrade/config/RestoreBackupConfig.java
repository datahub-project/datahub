package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
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
public class RestoreBackupConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "restoreBackup")
  @DependsOn({
    "ebeanServer",
    "entityService",
    "systemEntityClient",
    "graphService",
    "searchService",
    "entityRegistry"
  })
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public RestoreBackup createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService<?> entityService = applicationContext.getBean(EntityService.class);
    final SystemEntityClient entityClient = applicationContext.getBean(SystemEntityClient.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final EntitySearchService searchClient = applicationContext.getBean(EntitySearchService.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new RestoreBackup(
        ebeanServer, entityService, entityRegistry, entityClient, graphClient, searchClient);
  }

  @Bean(name = "restoreBackup")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public RestoreBackup createNotImplInstance() {
    log.warn("restoreIndices is not supported for cassandra!");
    return new RestoreBackup(null, null, null, null, null, null);
  }
}
