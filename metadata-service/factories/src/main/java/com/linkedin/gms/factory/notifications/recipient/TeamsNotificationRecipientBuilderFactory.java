package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.recipient.TeamsNotificationRecipientBuilder;
import com.linkedin.metadata.service.SettingsService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class TeamsNotificationRecipientBuilderFactory {
  @Autowired
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @Bean(name = "teamsNotificationRecipientBuilder")
  @Scope("singleton")
  @Nonnull
  protected TeamsNotificationRecipientBuilder getInstance() {
    return new TeamsNotificationRecipientBuilder(this.settingsService);
  }
}
