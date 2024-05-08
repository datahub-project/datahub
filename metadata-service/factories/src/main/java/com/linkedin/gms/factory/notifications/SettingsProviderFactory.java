package com.linkedin.gms.factory.notifications;

import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.entity.client.SystemEntityClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SettingsProviderFactory {
  @Bean(name = "settingsProvider")
  @Nonnull
  protected SettingsProvider getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return new SettingsProvider(systemEntityClient);
  }
}
