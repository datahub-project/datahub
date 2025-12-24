package com.datahub.notification.slack;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationSinkManager;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.proxy.SlackProxySink;
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
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SlackV2AssertionDeliverabilityTest {

  @Test
  public void testBroadcastAssertionStatusChangeProxiesWhenSlackV2Enabled() throws Exception {
    final OperationContext mockOpContext = mock(OperationContext.class);
    final IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Native Slack sink configured with slackSinkV2Enabled=true, which should exclude V2 templates
    // (including BROADCAST_ASSERTION_STATUS_CHANGE) from its supported template list.
    final MethodsClient mockSlackClient = mock(MethodsClient.class);
    final SlackNotificationSink nativeSlackSink = new SlackNotificationSink(mockSlackClient);
    nativeSlackSink.init(
        mockOpContext,
        new NotificationSinkConfig(
            ImmutableMap.of(
                "botToken", "abc", "defaultChannel", "#test", "slackSinkV2Enabled", "true"),
            mock(EntityClient.class),
            mock(SettingsProvider.class),
            mock(IdentityProvider.class),
            mock(EntityNameProvider.class),
            mock(SecretProvider.class),
            mock(ConnectionService.class),
            mockIntegrationsService,
            "http://localhost:9002"));

    // Slack v2 proxy sink should handle this template and forward to integrations-service.
    final SlackProxySink proxySink = new SlackProxySink();
    proxySink.init(
        mockOpContext,
        new NotificationSinkConfig(
            ImmutableMap.of(),
            mock(EntityClient.class),
            mock(SettingsProvider.class),
            mock(IdentityProvider.class),
            mock(EntityNameProvider.class),
            mock(SecretProvider.class),
            mock(ConnectionService.class),
            mockIntegrationsService,
            "http://localhost:9002"));

    final NotificationSinkManager manager =
        new NotificationSinkManager(List.of(nativeSlackSink, proxySink));

    final NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType
                    .BROADCAST_ASSERTION_STATUS_CHANGE)
            .setParameters(
                new com.linkedin.data.template.StringMap(
                    ImmutableMap.of(
                        "assertionUrn",
                        "urn:li:assertion:test",
                        "entityName",
                        "Sample",
                        "entityPath",
                        "/datasets/test",
                        "result",
                        "SUCCESS"))));
    request.setRecipients(
        new NotificationRecipientArray(
            List.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("U1"))));

    manager.handle(mockOpContext, request).get(15, TimeUnit.SECONDS);

    Mockito.verify(mockIntegrationsService, Mockito.times(1)).sendNotification(any());
    Mockito.verify(mockSlackClient, never()).chatPostMessage(any(ChatPostMessageRequest.class));
  }
}
