package com.linkedin.gms.factory.auth;

import com.datahub.authorization.role.RoleService;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RoleServiceFactory {

  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Bean(name = "roleService")
  @Scope("singleton")
  @Nonnull
  protected RoleService getInstance() throws Exception {
    return new RoleService(this._javaEntityClient);
  }
}
