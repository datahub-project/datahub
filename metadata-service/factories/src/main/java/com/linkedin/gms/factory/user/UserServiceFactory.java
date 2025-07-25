package com.linkedin.gms.factory.user;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.UserService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class UserServiceFactory {
  @Bean(name = "userService")
  @Scope("singleton")
  protected UserService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      @Qualifier("graphClient") final GraphClient graphClient) {
    return new UserService(entityClient, systemOperationContext, graphClient);
  }
}
