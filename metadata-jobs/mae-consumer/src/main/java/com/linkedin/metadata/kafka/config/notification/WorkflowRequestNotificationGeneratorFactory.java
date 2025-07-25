package com.linkedin.metadata.kafka.config.notification;

import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.actionrequest.ActionWorkflowServiceFactory;
import com.linkedin.gms.factory.common.GraphClientFactory;
import com.linkedin.gms.factory.notifications.recipient.NotificationRecipientBuildersFactory;
import com.linkedin.gms.factory.settings.SettingsServiceFactory;
import com.linkedin.gms.factory.user.UserServiceFactory;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.workflowrequest.WorkflowRequestNotificationGenerator;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.UserService;
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
  SettingsServiceFactory.class,
  NotificationRecipientBuildersFactory.class,
  ActionWorkflowServiceFactory.class,
  UserServiceFactory.class
})
public class WorkflowRequestNotificationGeneratorFactory {

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
  @Qualifier("actionWorkflowService")
  private ActionWorkflowService _actionWorkflowService;

  @Autowired
  @Qualifier("userService")
  private UserService _userService;

  @Bean(name = "workflowRequestNotificationGenerator")
  @Nonnull
  protected WorkflowRequestNotificationGenerator getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient) {
    return new WorkflowRequestNotificationGenerator(
        systemOpContext,
        _eventProducer,
        systemEntityClient,
        _graphClient,
        _settingsService,
        _notificationRecipientBuilders,
        _actionWorkflowService,
        _userService);
  }
}
