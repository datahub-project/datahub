package com.linkedin.gms.factory.application;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ApplicationService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ApplicationServiceFactory {

  @Bean(name = "applicationService")
  @Scope("singleton")
  @Nonnull
  protected ApplicationService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new ApplicationService(entityClient);
  }
}
