package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.secret.RotateSecrets;
import com.linkedin.metadata.entity.EntityService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class RotateSecretsConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "rotateSecrets")
  @DependsOn({"entityService"})
  @Nonnull
  public RotateSecrets createInstance() {
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    return new RotateSecrets(entityService);
  }
}
