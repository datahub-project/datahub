package com.linkedin.gms.factory.context.services;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SecretServiceConfiguration;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class SecretServiceFactory {

  @Value("${secretService.encryptionKey}")
  private String encryptionKey;

  @Bean(name = "dataHubSecretService")
  @Primary
  @Nonnull
  protected SecretService getInstance(final ConfigurationProvider configurationProvider) {
    SecretServiceConfiguration config = configurationProvider.getSecretService();
    SecretService.CallerGuardMode mode =
        config.getCallerGuardMode() != null
            ? SecretService.CallerGuardMode.valueOf(config.getCallerGuardMode().name())
            : SecretService.CallerGuardMode.ENFORCE;
    return new SecretService(this.encryptionKey, config.isV1AlgorithmEnabled(), mode);
  }
}
