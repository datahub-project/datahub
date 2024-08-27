package com.linkedin.gms.factory.settings;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.SettingsService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SettingsServiceFactory {
  @Bean(name = "settingsService")
  @Scope("singleton")
  @Nonnull
  protected SettingsService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new SettingsService(entityClient);
  }
}
