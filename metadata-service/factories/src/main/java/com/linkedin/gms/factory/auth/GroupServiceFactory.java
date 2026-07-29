package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

@Configuration
public class GroupServiceFactory {

  @Bean(name = "groupService")
  @Scope("singleton")
  @Nonnull
  protected GroupService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Lazy @Qualifier("entityService") final EntityService<?> entityService,
      @Lazy @Qualifier("graphClient") final GraphClient graphClient) {
    return new GroupService(systemEntityClient, entityService, graphClient);
  }
}
