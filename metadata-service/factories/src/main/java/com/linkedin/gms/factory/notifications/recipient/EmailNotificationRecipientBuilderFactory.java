package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.EmailNotificationRecipientBuilder;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EmailNotificationRecipientBuilderFactory {
  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider settingsProvider;

  @Bean(name = "emailNotificationRecipientBuilder")
  @Nonnull
  protected EmailNotificationRecipientBuilder getInstance() {
    return new EmailNotificationRecipientBuilder(this.settingsProvider);
  }
}
