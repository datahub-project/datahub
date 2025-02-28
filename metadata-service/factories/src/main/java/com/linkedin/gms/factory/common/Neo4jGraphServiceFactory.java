package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnProperty(name = "graphService.type", havingValue = "neo4j")
@Import({Neo4jDriverFactory.class})
public class Neo4jGraphServiceFactory {
  @Autowired
  @Qualifier("neo4jDriver")
  private Driver neo4jDriver;

  @Value("${neo4j.database}")
  private String neo4jDatabase;

  @Bean(name = "graphService")
  @Nonnull
  protected GraphService getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      final EntityRegistry entityRegistry) {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    return new Neo4jGraphService(
        systemOperationContext,
        lineageRegistry,
        neo4jDriver,
        SessionConfig.forDatabase(neo4jDatabase));
  }
}
