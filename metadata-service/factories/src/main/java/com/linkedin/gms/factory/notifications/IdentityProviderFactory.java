package com.linkedin.gms.factory.notifications;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.IdentityProvider;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IdentityProviderFactory {
  @Bean(name = "identityProvider")
  @Nonnull
  protected IdentityProvider getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return new IdentityProvider(systemEntityClient);
  }
}
