package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.recipient.EmailNotificationRecipientBuilder;
import com.linkedin.metadata.service.SettingsService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EmailNotificationRecipientBuilderFactory {
  @Autowired
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @Bean(name = "emailNotificationRecipientBuilder")
  @Nonnull
  protected EmailNotificationRecipientBuilder getInstance() {
    return new EmailNotificationRecipientBuilder(this.settingsService);
  }
}
