package com.linkedin.metadata.kafka.config.notification;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.notification.change.EntityChangeNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.incident.IncidentNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.ingestion.IngestionNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.proposal.ProposalNotificationGenerator;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({
  IncidentNotificationGeneratorFactory.class,
  ProposalNotificationGeneratorFactory.class,
  EntityChangeNotificationGeneratorFactory.class,
  IngestionNotificationGeneratorFactory.class
})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class NotificationGeneratorHookFactory {

  @Autowired private IncidentNotificationGenerator _incidentNotificationGenerator;

  @Autowired private ProposalNotificationGenerator _proposalNotificationGenerator;

  @Autowired private IngestionNotificationGenerator _ingestionNotificationGenerator;

  @Autowired private EntityChangeNotificationGenerator _entityChangeNotificationGenerator;

  @Value("${incidentNotification.enabled:true}")
  private boolean isEnabled;

  @Bean(name = "notificationGeneratorHook")
  @Scope("singleton")
  @Nonnull
  protected NotificationGeneratorHook getInstance() {
    return new NotificationGeneratorHook(
        ImmutableList.of(
            _incidentNotificationGenerator,
            _proposalNotificationGenerator,
            _entityChangeNotificationGenerator,
            _ingestionNotificationGenerator),
        isEnabled);
  }
}
