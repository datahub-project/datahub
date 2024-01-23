package com.linkedin.gms.factory.auth;

import com.datahub.authorization.role.RoleService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RoleServiceFactory {

  @Bean(name = "roleService")
  @Scope("singleton")
  @Nonnull
  protected RoleService getInstance(@Qualifier("entityClient") final EntityClient entityClient)
      throws Exception {
    return new RoleService(entityClient);
  }
}
