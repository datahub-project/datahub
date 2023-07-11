package com.linkedin.metadata.kafka.config.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.SlackNotificationRecipientBuilder;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.gms.factory.timeline.EntityChangeEventGeneratorRegistryFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.change.EntityChangeNotificationGenerator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({SystemAuthenticationFactory.class, RestliEntityClientFactory.class, GraphClientFactory.class,
    SettingsProviderFactory.class, EntityRegistryFactory.class, EntityChangeEventGeneratorRegistryFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class EntityChangeNotificationGeneratorFactory {
  @Autowired
  @Qualifier("entityChangeEventGeneratorRegistry")
  private EntityChangeEventGeneratorRegistry _entityChangeEventGeneratorRegistry;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Autowired
  @Qualifier("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Autowired
  @Qualifier("restliEntityClient")
  private RestliEntityClient _entityClient;

  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider _settingsProvider;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _systemAuthentication;

  @Autowired
  @Qualifier("slackNotificationRecipientBuilder")
  private SlackNotificationRecipientBuilder _slackNotificationRecipientBuilder;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider _configProvider;


  @Bean(name = "entityChangeNotificationGenerator")
  @Scope("singleton")
  @Nonnull
  protected EntityChangeNotificationGenerator getInstance() {
    return new EntityChangeNotificationGenerator(
        _entityChangeEventGeneratorRegistry,
        _entityRegistry,
        _eventProducer,
        _entityClient,
        _graphClient,
        _settingsProvider,
        _systemAuthentication,
        _slackNotificationRecipientBuilder,
        _configProvider.getFeatureFlags()
    );
  }
}