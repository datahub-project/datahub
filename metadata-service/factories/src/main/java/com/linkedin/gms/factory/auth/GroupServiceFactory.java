package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
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
public class GroupServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "groupService")
  @Scope("singleton")
  @Nonnull
  protected GroupService getInstance() throws Exception {
    return new GroupService(this._javaEntityClient, this._entityService, this._graphClient);
  }
}
