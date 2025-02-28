package com.linkedin.metadata.kafka.config.notification;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.notification.change.EntityChangeNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.connection.ConnectionTestNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.incident.IncidentNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.ingestion.IngestionNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.proposal.ProposalNotificationGenerator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  IncidentNotificationGeneratorFactory.class,
  ProposalNotificationGeneratorFactory.class,
  EntityChangeNotificationGeneratorFactory.class,
  IngestionNotificationGeneratorFactory.class,
  ConnectionTestNotificationGeneratorFactory.class
})
public class NotificationGeneratorHookFactory {

  @Autowired private IncidentNotificationGenerator _incidentNotificationGenerator;

  @Autowired private ProposalNotificationGenerator _proposalNotificationGenerator;

  @Autowired private IngestionNotificationGenerator _ingestionNotificationGenerator;

  @Autowired private EntityChangeNotificationGenerator _entityChangeNotificationGenerator;

  @Autowired private ConnectionTestNotificationGenerator _connectionTestNotificationGenerator;

  @Value("${notifications.enabled:true}")
  private boolean isEnabled;

  @Bean(name = "notificationGeneratorHook")
  @Nonnull
  protected NotificationGeneratorHook getInstance(
      @Nonnull @Value("${notifications.consumerGroupSuffix}") String consumerGroupSuffix) {
    return new NotificationGeneratorHook(
        ImmutableList.of(
            _incidentNotificationGenerator,
            _proposalNotificationGenerator,
            _entityChangeNotificationGenerator,
            _ingestionNotificationGenerator,
            _connectionTestNotificationGenerator),
        isEnabled,
        consumerGroupSuffix);
  }
}
