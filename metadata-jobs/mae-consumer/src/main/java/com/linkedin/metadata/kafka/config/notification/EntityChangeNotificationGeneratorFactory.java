package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.notifications.recipient.NotificationRecipientBuildersFactory;
import com.linkedin.gms.factory.settings.SettingsServiceFactory;
import com.linkedin.gms.factory.timeline.eventgenerator.EntityChangeEventGeneratorRegistryFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.change.EntityChangeNotificationGenerator;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({
  GraphClientFactory.class,
  SettingsServiceFactory.class,
  EntityChangeEventGeneratorRegistryFactory.class,
  NotificationRecipientBuildersFactory.class
})
public class EntityChangeNotificationGeneratorFactory {
  @Autowired
  @Qualifier("entityChangeEventGeneratorRegistry")
  private EntityChangeEventGeneratorRegistry _entityChangeEventGeneratorRegistry;

  @Autowired
  @Qualifier("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Autowired
  @Qualifier("settingsService")
  private SettingsService _settingsService;

  @Autowired
  @Qualifier("notificationRecipientBuilders")
  private NotificationRecipientBuilders _notificationRecipientBuilders;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider _configProvider;

  @Autowired
  @Qualifier("assertionService")
  private AssertionService _assertionService;

  @Bean(name = "entityChangeNotificationGenerator")
  @Scope("singleton")
  @Nonnull
  protected EntityChangeNotificationGenerator getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient) {
    return new EntityChangeNotificationGenerator(
        systemOpContext,
        _entityChangeEventGeneratorRegistry,
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsService,
        _assertionService,
        _notificationRecipientBuilders,
        _configProvider.getFeatureFlags());
  }
}
