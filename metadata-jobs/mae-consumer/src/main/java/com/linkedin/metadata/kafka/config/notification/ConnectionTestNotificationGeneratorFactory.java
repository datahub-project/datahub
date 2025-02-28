package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.kafka.DataHubKafkaEventProducerFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.notifications.recipient.NotificationRecipientBuildersFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.connection.ConnectionTestNotificationGenerator;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
public class ConnectionTestNotificationGeneratorFactory {

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
  @Value("${connections.testExecutionRequestHook.enabled:true}")
  private boolean isEnabled;

  @Bean(name = "connectionTestNotificationGenerator")
  @Nonnull
  protected ConnectionTestNotificationGenerator getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient) {
    return new ConnectionTestNotificationGenerator(
        systemOpContext,
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsProvider,
        _notificationRecipientBuilders,
        isEnabled);
  }
}
