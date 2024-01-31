package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SlackNotificationRecipientBuilderFactory {
  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider settingsProvider;

  @Bean(name = "slackNotificationRecipientBuilder")
  @Scope("singleton")
  @Nonnull
  protected SlackNotificationRecipientBuilder getInstance(
      final SystemEntityClient systemEntityClient) {
    return new SlackNotificationRecipientBuilder(
        this.settingsProvider, systemEntityClient, systemEntityClient.getSystemAuthentication());
  }
}
