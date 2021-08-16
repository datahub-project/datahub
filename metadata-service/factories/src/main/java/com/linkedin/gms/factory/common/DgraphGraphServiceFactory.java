package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.DgraphGraphService;
import com.linkedin.metadata.graph.Neo4jGraphService;
import io.dgraph.DgraphClient;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.Nonnull;


@Configuration
@Import({DgraphClientFactory.class})
public class DgraphGraphServiceFactory {
  @Autowired
  @Qualifier("dgraphClient")
  private DgraphClient dgraphClient;

  @Bean(name = "dgraphGraphService")
  @Nonnull
  protected DgraphGraphService getInstance() {
    return new DgraphGraphService(dgraphClient);
  }
}
