package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.JavaGraphClient;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class GraphClientFactory {

  @Nonnull
  @Bean(name = "graphClient")
  @Primary
  protected GraphClient createInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final GraphService graphService) {
    return new JavaGraphClient(systemOpContext, graphService);
  }
}
