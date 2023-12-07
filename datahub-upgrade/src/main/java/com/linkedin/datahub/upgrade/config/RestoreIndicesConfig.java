package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
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
public class RestoreIndicesConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "restoreIndices")
  @DependsOn({"ebeanServer", "entityService", "searchService", "graphService", "entityRegistry"})
  @Nonnull
  public RestoreIndices createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final EntitySearchService entitySearchService =
        applicationContext.getBean(EntitySearchService.class);
    final GraphService graphService = applicationContext.getBean(GraphService.class);
    final EntityRegistry entityRegistry = applicationContext.getBean(EntityRegistry.class);

    return new RestoreIndices(
        ebeanServer, entityService, entityRegistry, entitySearchService, graphService);
  }
}
