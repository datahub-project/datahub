package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
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

  @Autowired
  @Qualifier("restliEntityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Bean(name = "slackNotificationRecipientBuilder")
  @Scope("singleton")
  @Nonnull
  protected SlackNotificationRecipientBuilder getInstance() {
    return new SlackNotificationRecipientBuilder(this.settingsProvider, this.entityClient, this.systemAuthentication);
  }
}
