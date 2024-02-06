package com.linkedin.gms.factory.notifications;

import com.datahub.notification.provider.IdentityProvider;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IdentityProviderFactory {
  @Bean(name = "identityProvider")
  @Scope("singleton")
  @Nonnull
  protected IdentityProvider getInstance(final SystemEntityClient systemEntityClient) {
    return new IdentityProvider(systemEntityClient, systemEntityClient.getSystemAuthentication());
  }
}
