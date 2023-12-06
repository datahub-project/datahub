package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.JavaGraphClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({GraphServiceFactory.class})
public class GraphClientFactory {
  @Autowired
  @Qualifier("graphService")
  private GraphService _graphService;

  @Nonnull
  @DependsOn({"graphService"})
  @Bean(name = "graphClient")
  @Primary
  protected GraphClient createInstance() {
    return new JavaGraphClient(_graphService);
  }
}
