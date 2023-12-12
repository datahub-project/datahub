package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class RestoreBackupConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "restoreBackup")
  @DependsOn({
    "ebeanServer",
    "entityService",
    "systemRestliEntityClient",
    "graphService",
    "searchService",
    "entityRegistry"
  })
  @Nonnull
  public RestoreBackup createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final SystemRestliEntityClient entityClient =
        applicationContext.getBean(SystemRestliEntityClient.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final EntitySearchService searchClient = applicationContext.getBean(EntitySearchService.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new RestoreBackup(
        ebeanServer, entityService, entityRegistry, entityClient, graphClient, searchClient);
  }
}
