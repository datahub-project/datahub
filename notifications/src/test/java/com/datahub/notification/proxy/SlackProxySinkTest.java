package com.datahub.notification.proxy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSinkConfig;
import com.google.common.collect.ImmutableMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.EntityNameProvider;
import com.linkedin.metadata.service.IdentityProvider;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SlackProxySinkTest {

  @Test
  public void testSendProxiesToIntegrationsService() throws Exception {
    final IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    final SlackProxySink sink = new SlackProxySink();
    sink.init(
        mock(OperationContext.class),
        new NotificationSinkConfig(
            Collections.emptyMap(),
            mock(EntityClient.class),
            mock(com.datahub.notification.provider.SettingsProvider.class),
            mock(IdentityProvider.class),
            mock(EntityNameProvider.class),
            mock(com.datahub.notification.provider.SecretProvider.class),
            mock(ConnectionService.class),
            mockIntegrationsService,
            "http://localhost:9002"));

    final NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType
                    .BROADCAST_ASSERTION_STATUS_CHANGE)
            .setParameters(new com.linkedin.data.template.StringMap(ImmutableMap.of())));
    request.setRecipients(
        new NotificationRecipientArray(
            Collections.singletonList(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("U123"))));

    sink.send(mock(OperationContext.class), request, new NotificationContext());

    Mockito.verify(mockIntegrationsService, Mockito.times(1)).sendNotification(any());
  }
}
