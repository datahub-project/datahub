package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaEventProducerFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.notifications.recipient.NotificationRecipientBuildersFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.incident.IncidentNotificationGenerator;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  GraphClientFactory.class,
  SettingsProviderFactory.class,
  DataHubKafkaEventProducerFactory.class,
  KafkaHealthChecker.class,
  NotificationRecipientBuildersFactory.class
})
public class IncidentNotificationGeneratorFactory {

  @Autowired
  @Qualifier("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider _settingsProvider;

  @Autowired
  @Qualifier("notificationRecipientBuilders")
  private NotificationRecipientBuilders _notificationRecipientBuilders;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider _configProvider;

  @Bean(name = "incidentNotificationGenerator")
  @Nonnull
  protected IncidentNotificationGenerator getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient) {
    return new IncidentNotificationGenerator(
        systemOpContext,
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsProvider,
        _notificationRecipientBuilders,
        _configProvider.getFeatureFlags());
  }
}
