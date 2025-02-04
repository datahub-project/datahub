package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import javax.annotation.Nonnull;

import com.linkedin.metadata.service.SettingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SlackNotificationRecipientBuilderFactory {
  @Autowired
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @Bean(name = "slackNotificationRecipientBuilder")
  @Scope("singleton")
  @Nonnull
  protected SlackNotificationRecipientBuilder getInstance() {
    return new SlackNotificationRecipientBuilder(this.settingsService);
  }
}
