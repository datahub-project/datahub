package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.notifications.recipient.NotificationRecipientBuildersFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.proposal.ProposalNotificationGenerator;
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
  NotificationRecipientBuildersFactory.class
})
public class ProposalNotificationGeneratorFactory {

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

  @Bean(name = "proposalNotificationGenerator")
  @Nonnull
  protected ProposalNotificationGenerator getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient) {
    return new ProposalNotificationGenerator(
        systemOpContext,
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsProvider,
        _notificationRecipientBuilders,
        _configProvider.getFeatureFlags());
  }
}
