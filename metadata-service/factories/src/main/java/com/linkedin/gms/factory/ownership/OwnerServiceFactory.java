package com.linkedin.gms.factory.ownership;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.OwnerService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class OwnerServiceFactory {
  @Bean(name = "ownerService")
  @Scope("singleton")
  @Nonnull
  protected OwnerService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient) throws Exception {
    return new OwnerService(entityClient);
  }
}
