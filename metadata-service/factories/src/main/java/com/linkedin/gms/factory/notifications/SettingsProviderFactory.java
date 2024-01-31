package com.linkedin.gms.factory.notifications;

import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SettingsProviderFactory {
  @Bean(name = "settingsProvider")
  @Scope("singleton")
  @Nonnull
  protected SettingsProvider getInstance(final SystemEntityClient systemEntityClient) {
    return new SettingsProvider(systemEntityClient, systemEntityClient.getSystemAuthentication());
  }
}
