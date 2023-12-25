package com.linkedin.datahub.upgrade.config;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.datahub.upgrade.nocodecleanup.NoCodeCleanupUpgrade;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class NoCodeCleanupConfig {

  @Autowired ApplicationContext applicationContext;

  @Bean(name = "noCodeCleanup")
  @DependsOn({
    "ebeanServer",
    "graphService",
    "elasticSearchRestHighLevelClient",
    INDEX_CONVENTION_BEAN
  })
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public NoCodeCleanupUpgrade createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final RestHighLevelClient searchClient = applicationContext.getBean(RestHighLevelClient.class);
    final IndexConvention indexConvention = applicationContext.getBean(IndexConvention.class);
    return new NoCodeCleanupUpgrade(ebeanServer, graphClient, searchClient, indexConvention);
  }

  @Bean(name = "noCodeCleanup")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public NoCodeCleanupUpgrade createNotImplInstance() {
    log.warn("NoCode is not supported for cassandra!");
    return new NoCodeCleanupUpgrade(null, null, null, null);
  }
}
