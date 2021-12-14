package com.linkedin.datahub.upgrade.config;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
  @DependsOn({"ebeanServer", "entityService", "systemAuthentication", "restliEntityClient", "graphService",
      "searchService", "entityRegistry"})
  @Nonnull
  public RestoreBackup createInstance() {
    final EbeanServer ebeanServer = applicationContext.getBean(EbeanServer.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final Authentication systemAuthentication = applicationContext.getBean(Authentication.class);
    final RestliEntityClient entityClient = applicationContext.getBean(RestliEntityClient.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final EntitySearchService searchClient = applicationContext.getBean(EntitySearchService.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new RestoreBackup(ebeanServer, entityService, entityRegistry, systemAuthentication, entityClient,
        graphClient, searchClient);
  }
}
