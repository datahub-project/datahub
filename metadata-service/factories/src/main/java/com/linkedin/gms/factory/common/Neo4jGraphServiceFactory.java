package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.Neo4jGraphService;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({Neo4jDriverFactory.class})
public class Neo4jGraphServiceFactory {
  @Autowired
  @Qualifier("neo4jDriver")
  private Driver neo4jDriver;

  @Bean(name = "neo4jGraphService")
  @Nonnull
  protected Neo4jGraphService getInstance() {
    return new Neo4jGraphService(neo4jDriver);
  }
}
