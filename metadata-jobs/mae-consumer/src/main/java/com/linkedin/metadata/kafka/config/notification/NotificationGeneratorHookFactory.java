package com.linkedin.metadata.kafka.config.notification;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.notification.incident.IncidentNotificationGenerator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({IncidentNotificationGeneratorFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class NotificationGeneratorHookFactory {

  @Autowired
  private IncidentNotificationGenerator _incidentNotificationGenerator;

  @Bean(name = "notificationGeneratorHook")
  @Scope("singleton")
  @Nonnull
  protected NotificationGeneratorHook getInstance() {
    return new NotificationGeneratorHook(
        ImmutableList.of(_incidentNotificationGenerator)
    );
  }
}