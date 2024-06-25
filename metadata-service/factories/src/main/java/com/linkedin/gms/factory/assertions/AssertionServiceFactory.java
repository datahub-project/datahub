package com.linkedin.gms.factory.assertions;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class AssertionServiceFactory {

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "assertionService")
  @Scope("singleton")
  @Nonnull
  protected AssertionService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient)
      throws Exception {
    return new AssertionService(systemEntityClient, _graphClient);
  }
}
