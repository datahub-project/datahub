package com.linkedin.gms.factory.notifications;

import com.datahub.notification.provider.SecretProvider;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.context.services.SecretServiceFactory;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SecretServiceFactory.class})
public class SecretProviderFactory {
  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @Bean(name = "secretProvider")
  @Nonnull
  protected SecretProvider getInstance(final SystemEntityClient systemEntityClient) {
    return new SecretProvider(systemEntityClient, this.secretService);
  }
}
