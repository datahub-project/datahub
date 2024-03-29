package com.linkedin.gms.factory.context.services;

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
  protected SecretService getInstance() {
    return new SecretService(this.encryptionKey);
  }
}
