package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({Neo4jGraphServiceFactory.class, ElasticSearchGraphServiceFactory.class})
public class GraphServiceFactory {
  @Autowired
  @Qualifier("elasticSearchGraphService")
  private ElasticSearchGraphService _elasticSearchGraphService;

  @Autowired
  @Qualifier("neo4jGraphService")
  private Neo4jGraphService _neo4jGraphService;

  @Value("${graphService.type}")
  private String graphServiceImpl;

  @Nonnull
  @DependsOn({"neo4jGraphService", "elasticSearchGraphService"})
  @Bean(name = "graphService")
  @Primary
  protected GraphService createInstance() {
    if (graphServiceImpl.equalsIgnoreCase("neo4j")) {
      return _neo4jGraphService;
    } else if (graphServiceImpl.equalsIgnoreCase("elasticsearch")) {
      return _elasticSearchGraphService;
    } else {
      throw new RuntimeException(
          "Error: Failed to initialize graph service. Graph Service provided: "
              + graphServiceImpl
              + ". Valid options: [neo4j, elasticsearch].");
    }
  }
}
