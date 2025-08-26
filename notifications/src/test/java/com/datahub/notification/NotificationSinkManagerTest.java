package com.datahub.notification;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NotificationSinkManagerTest {

  @Test
  public void testHandleEnabledEligibleTemplate() throws Exception {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates())
        .thenReturn(ImmutableList.of(NotificationTemplateType.CUSTOM));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);
    Mockito.when(notificationSink.recipientTypes())
        .thenReturn(ImmutableList.of(NotificationRecipientType.SLACK_CHANNEL));

    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.ENABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.CUSTOM)
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title",
                        "body", "Test Body"))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_CHANNEL)
                    .setId("#channel"))));

    manager.handle(mock(OperationContext.class), request).join();

    // Verify that "send" was called on the sink.
    Mockito.verify(notificationSink, Mockito.times(1))
        .send(any(OperationContext.class), Mockito.eq(request), Mockito.any());
  }

  @Test
  public void testHandleEnabledTemplateNotSupported() throws Exception {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates())
        .thenReturn(Collections.emptyList()); // Sink doesn't support any templates!
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.ENABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.CUSTOM)
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title",
                        "body", "Test Body"))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId("urn:li:corpuser:test"))));

    manager.handle(mock(OperationContext.class), request).join();

    // Verify that "send" was NOT called on the sink. (It cannot handle the template)
    Mockito.verify(notificationSink, Mockito.times(0))
        .send(any(OperationContext.class), Mockito.same(request), Mockito.any());
  }

  @Test
  public void testHandleEnabledUnknownTemplate() {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(Collections.emptyList());
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.ENABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.INVALID_TEMPLATE)
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title",
                        "body", "Test Body"))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId("urn:li:corpuser:test"))));

    Assert.assertThrows(
        RuntimeException.class, () -> manager.handle(mock(OperationContext.class), request));
  }

  @Test
  public void testHandleEnabledMissingRequiredTemplateParams() {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates())
        .thenReturn(ImmutableList.of(NotificationTemplateType.CUSTOM));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.ENABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.INVALID_TEMPLATE)
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title"
                        // "body", "Test Body" // Missing required "body" parameter for template
                        // type CUSTOM
                        ))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId("urn:li:corpuser:test"))));

    Assert.assertThrows(
        RuntimeException.class, () -> manager.handle(mock(OperationContext.class), request));
  }

  @Test
  public void testHandleManagerDisabled() throws Exception {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates())
        .thenReturn(ImmutableList.of(NotificationTemplateType.CUSTOM));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    // Create manager in "disabled" state.
    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.DISABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.CUSTOM.toString()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title",
                        "body", "Test Body"))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId("urn:li:corpuser:test"))));

    manager.handle(mock(OperationContext.class), request).join();

    // Verify that "send" was NOT called on the sink.
    Mockito.verify(notificationSink, Mockito.times(0))
        .send(any(OperationContext.class), Mockito.same(request), Mockito.any());
  }

  @Test
  public void testHandleFilterRecipientByType() throws Exception {
    NotificationSink notificationSink = mock(NotificationSink.class);
    Mockito.when(notificationSink.templates())
        .thenReturn(ImmutableList.of(NotificationTemplateType.CUSTOM));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);
    Mockito.when(notificationSink.recipientTypes())
        .thenReturn(ImmutableList.of(NotificationRecipientType.SLACK_CHANNEL));

    // Create manager in "enabled" state.
    NotificationSinkManager manager =
        new NotificationSinkManager(
            NotificationSinkManager.NotificationManagerMode.ENABLED,
            ImmutableList.of(notificationSink));

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(
        new NotificationMessage()
            .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.CUSTOM)
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "title", "Test Title",
                        "body", "Test Body"))));
    request.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_CHANNEL)
                    .setId("#custom-slack-channel"),
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId("urn:li:corpuser:test"))));

    manager.handle(mock(OperationContext.class), request).join();

    final NotificationRequest expectedRequest =
        new NotificationRequest()
            .setMessage(request.getMessage())
            .setRecipients(
                new NotificationRecipientArray(
                    ImmutableList.of(
                        new NotificationRecipient()
                            .setType(NotificationRecipientType.SLACK_CHANNEL)
                            .setId("#custom-slack-channel"))));

    // Verify that "send" was called with the appropriate recipient types
    Mockito.verify(notificationSink, Mockito.times(1))
        .send(any(OperationContext.class), Mockito.eq(expectedRequest), Mockito.any());
  }
}
