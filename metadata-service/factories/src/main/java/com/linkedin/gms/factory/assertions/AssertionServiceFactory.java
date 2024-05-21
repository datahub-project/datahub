package com.linkedin.gms.factory.assertions;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.AssertionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AssertionServiceFactory {
  @Bean(name = "assertionService")
  @Nonnull
  protected AssertionService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient)
      throws Exception {
    return new AssertionService(systemEntityClient);
  }
}
