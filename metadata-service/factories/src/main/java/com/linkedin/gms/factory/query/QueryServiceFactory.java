package com.linkedin.gms.factory.query;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.QueryService;
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
public class QueryServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "queryService")
  @Scope("singleton")
  @Nonnull
  protected QueryService getInstance() throws Exception {
    return new QueryService(_javaEntityClient, _authentication);
  }
}
