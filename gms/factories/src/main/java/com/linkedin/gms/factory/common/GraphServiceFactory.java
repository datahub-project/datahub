package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.Neo4jGraphService;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;


@Configuration
@Import({Neo4jDriverFactory.class})
public class GraphServiceFactory {
  @Autowired
  @Qualifier("neo4jDriver")
  private Driver neo4jDriver;

  @Nonnull
  @DependsOn({"neo4jDriver"})
  @Bean(name = "graphService")
  protected GraphService createInstance() {
    return new Neo4jGraphService(neo4jDriver);
  }
}
