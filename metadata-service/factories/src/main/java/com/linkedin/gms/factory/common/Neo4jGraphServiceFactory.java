package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({Neo4jDriverFactory.class, EntityRegistryFactory.class})
public class Neo4jGraphServiceFactory {
  @Autowired
  @Qualifier("neo4jDriver")
  private Driver neo4jDriver;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean(name = "neo4jGraphService")
  @Nonnull
  protected Neo4jGraphService getInstance() {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    return new Neo4jGraphService(lineageRegistry, neo4jDriver);
  }
}
