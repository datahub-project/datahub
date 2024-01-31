package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.ingestion.IngestionNotificationGenerator;
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
@Import({GraphClientFactory.class, SettingsProviderFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IngestionNotificationGeneratorFactory {

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
  @Qualifier("slackNotificationRecipientBuilder")
  private SlackNotificationRecipientBuilder _slackNotificationRecipientBuilder;

  @Bean(name = "ingestionNotificationGenerator")
  @Scope("singleton")
  @Nonnull
  protected IngestionNotificationGenerator getInstance(
      final SystemEntityClient systemEntityClient) {
    return new IngestionNotificationGenerator(
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsProvider,
        _slackNotificationRecipientBuilder,
        systemEntityClient.getSystemAuthentication());
  }
}
