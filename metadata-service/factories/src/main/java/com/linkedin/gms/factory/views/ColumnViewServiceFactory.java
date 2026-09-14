package com.linkedin.gms.factory.views;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ColumnViewService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ColumnViewServiceFactory {

  @Bean(name = "columnViewService")
  @Scope("singleton")
  @Nonnull
  protected ColumnViewService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new ColumnViewService(entityClient);
  }
}
