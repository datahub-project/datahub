package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
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
public class RestoreIndicesConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "restoreIndices")
  @DependsOn({"ebeanServer", "entityService", "searchService", "graphService"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public RestoreIndices createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final EntityService<?> entityService = applicationContext.getBean(EntityService.class);
    final EntitySearchService entitySearchService =
        applicationContext.getBean(EntitySearchService.class);
    final GraphService graphService = applicationContext.getBean(GraphService.class);

    return new RestoreIndices(ebeanServer, entityService, entitySearchService, graphService);
  }

  @Bean(name = "restoreIndices")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public RestoreIndices createNotImplInstance() {
    log.warn("restoreIndices is not supported for cassandra!");
    return new RestoreIndices(null, null, null, null);
  }
}
