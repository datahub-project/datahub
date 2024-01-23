package com.linkedin.gms.factory.query;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class QueryServiceFactory {

  @Bean(name = "queryService")
  @Scope("singleton")
  @Nonnull
  protected QueryService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new QueryService(entityClient, entityClient.getSystemAuthentication());
  }
}
