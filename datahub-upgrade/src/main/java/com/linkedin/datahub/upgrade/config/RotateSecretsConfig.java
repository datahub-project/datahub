package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.secret.RotateSecrets;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RotateSecretsConfig {

  @Bean(name = "rotateSecrets")
  @Nonnull
  public RotateSecrets createInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      final EntityService<?> entityService,
      final ConfigurationProvider configurationProvider) {
    return new RotateSecrets(
        systemOperationContext, entityService, configurationProvider.getSecretService());
  }
}
