package com.linkedin.gms.factory.application;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.ApplicationService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ApplicationServiceFactory {

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "applicationService")
  @Scope("singleton")
  @Nonnull
  protected ApplicationService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new ApplicationService(entityClient, _graphClient);
  }
}
