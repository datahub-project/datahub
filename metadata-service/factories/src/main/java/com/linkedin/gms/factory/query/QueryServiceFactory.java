package com.linkedin.gms.factory.query;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.QueryService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class QueryServiceFactory {

  @Bean(name = "queryService")
  @Scope("singleton")
  @Nonnull
  protected QueryService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new QueryService(entityClient);
  }
}
