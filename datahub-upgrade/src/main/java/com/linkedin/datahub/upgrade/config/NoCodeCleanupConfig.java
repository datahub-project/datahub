package com.linkedin.datahub.upgrade.config;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.datahub.upgrade.nocodecleanup.NoCodeCleanupUpgrade;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

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
  @Nonnull
  public NoCodeCleanupUpgrade createInstance() {
    final Database ebeanServer = applicationContext.getBean(Database.class);
    final GraphService graphClient = applicationContext.getBean(GraphService.class);
    final RestHighLevelClient searchClient = applicationContext.getBean(RestHighLevelClient.class);
    final IndexConvention indexConvention = applicationContext.getBean(IndexConvention.class);
    return new NoCodeCleanupUpgrade(ebeanServer, graphClient, searchClient, indexConvention);
  }
}
