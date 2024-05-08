package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class GroupServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "groupService")
  @Scope("singleton")
  @Nonnull
  protected GroupService getInstance(@Qualifier("entityClient") final EntityClient entityClient)
      throws Exception {
    return new GroupService(entityClient, _entityService, _graphClient);
  }
}
