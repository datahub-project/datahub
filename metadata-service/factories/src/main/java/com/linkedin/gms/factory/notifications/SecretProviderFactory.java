package com.linkedin.gms.factory.notifications;

import com.datahub.notification.provider.SecretProvider;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.secret.SecretServiceFactory;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({SecretServiceFactory.class})
public class SecretProviderFactory {
  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @Bean(name = "secretProvider")
  @Scope("singleton")
  @Nonnull
  protected SecretProvider getInstance(final SystemEntityClient systemEntityClient) {
    return new SecretProvider(
        systemEntityClient, systemEntityClient.getSystemAuthentication(), this.secretService);
  }
}
