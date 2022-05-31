package com.linkedin.metadata.kafka.config.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.notifications.SettingsProviderFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.proposal.ProposalNotificationGenerator;
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
    SettingsProviderFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ProposalNotificationGeneratorFactory {

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

  @Bean(name = "proposalNotificationGenerator")
  @Scope("singleton")
  @Nonnull
  protected ProposalNotificationGenerator getInstance() {
    return new ProposalNotificationGenerator(
        _eventProducer,
        _entityClient,
        _graphClient,
        _settingsProvider,
        _systemAuthentication
    );
  }
}