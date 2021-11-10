package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.ebean.EbeanServer;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class RestoreBackupConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "restoreBackup")
  @DependsOn({"ebeanServer", "entityService", "entityClient", "graphService", "searchService"})
  @Nonnull
  public RestoreBackup createInstance() {
    final EbeanServer ebeanServer = applicationContext.getBean(EbeanServer.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final EntityClient entityClient = applicationContext.getBean(EntityClient.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final EntitySearchService searchClient = applicationContext.getBean(EntitySearchService.class);

    return new RestoreBackup(ebeanServer, entityService, SnapshotEntityRegistry.getInstance(), entityClient,
        graphClient, searchClient);
  }
}
