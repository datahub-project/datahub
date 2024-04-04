package com.linkedin.gms.factory.notifications.recipient;

import com.datahub.notification.recipient.EmailNotificationRecipientBuilder;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import com.google.common.collect.ImmutableMap;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({
  SlackNotificationRecipientBuilderFactory.class,
  EmailNotificationRecipientBuilderFactory.class
})
public class NotificationRecipientBuildersFactory {
  @Autowired
  @Qualifier("slackNotificationRecipientBuilder")
  private SlackNotificationRecipientBuilder slackNotificationRecipientBuilder;

  @Autowired
  @Qualifier("emailNotificationRecipientBuilder")
  private EmailNotificationRecipientBuilder emailNotificationRecipientBuilder;

  @Bean(name = "notificationRecipientBuilders")
  @Scope("singleton")
  @Nonnull
  protected NotificationRecipientBuilders getInstance() {
    return new NotificationRecipientBuilders(
        ImmutableMap.of(
            NotificationSinkType.SLACK, this.slackNotificationRecipientBuilder,
            NotificationSinkType.EMAIL, this.emailNotificationRecipientBuilder));
  }
}
