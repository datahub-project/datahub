package com.linkedin.gms.factory.auth;

import com.datahub.authorization.role.RoleService;
import com.linkedin.entity.client.EntityClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class RoleServiceFactory {

  @Bean(name = "roleService")
  @Scope("singleton")
  @Nonnull
  protected RoleService getInstance(@Qualifier("entityClient") final EntityClient entityClient)
      throws Exception {
    return new RoleService(entityClient);
  }
}
