package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.dgraph.DgraphGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.dgraph.DgraphClient;
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

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean(name = "dgraphGraphService")
  @Nonnull
  protected DgraphGraphService getInstance() {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    return new DgraphGraphService(lineageRegistry, dgraphClient);
  }
}
