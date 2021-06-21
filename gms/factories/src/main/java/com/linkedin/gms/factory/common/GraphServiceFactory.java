package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.Neo4jGraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;


@Configuration
@Import({Neo4jDriverFactory.class, ElasticSearchGraphServiceFactory.class})
public class GraphServiceFactory {
  @Autowired
  @Qualifier("elasticSearchGraphService")
  private ElasticSearchGraphService _elasticSearchGraphService;

  @Autowired
  @Qualifier("neo4jDriver")
  private Driver neo4jDriver;

  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Value("${GRAPH_SERVICE:neo4j}")
  private String graphService;

  @Nonnull
  @DependsOn({"neo4jDriver", "elasticSearchGraphService"})
  @Bean(name = "graphService")
  @Primary
  protected GraphService createInstance() {
    if (graphService.equalsIgnoreCase("neo4j")) {
      return new Neo4jGraphService(neo4jDriver);
    } else if (graphService.equalsIgnoreCase("elasticsearch")) {
      return _elasticSearchGraphService;
    } else {
      throw new RuntimeException(
          "Error: Failed to initialize graph service. Graph Service provided: " + graphService
              + ". Valid options: [neo4j, elasticsearch].");
    }
  }
}
