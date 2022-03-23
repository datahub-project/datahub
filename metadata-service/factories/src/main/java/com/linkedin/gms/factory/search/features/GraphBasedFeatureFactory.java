package com.linkedin.gms.factory.search.features;

import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.features.GraphBasedFeature;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import(GraphServiceFactory.class)
public class GraphBasedFeatureFactory {
  @Autowired
  private GraphService graphService;

  @Bean(name = "graphBasedFeature")
  @Nonnull
  protected GraphBasedFeature getInstance() {
    return new GraphBasedFeature(graphService);
  }
}
