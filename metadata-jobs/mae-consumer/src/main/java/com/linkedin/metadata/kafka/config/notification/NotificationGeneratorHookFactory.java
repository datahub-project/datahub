package com.linkedin.metadata.kafka.config.notification;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.notification.incident.IncidentNotificationGenerator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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