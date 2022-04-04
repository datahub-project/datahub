package com.datahub.notification;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NotificationSinkManagerTest {

  @Test
  public void testHandleEnabledEligibleTemplate() throws Exception {
    NotificationSink notificationSink = Mockito.mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(ImmutableList.of(
       NotificationTemplateType.CUSTOM
    ));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager = new NotificationSinkManager(
      NotificationSinkManager.NotificationManagerMode.ENABLED,
      ImmutableList.of(notificationSink)
    );

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(new NotificationMessage()
      .setTemplate(NotificationTemplateType.CUSTOM.toString())
      .setParameters(new StringMap(
          ImmutableMap.of(
              "title", "Test Title",
              "body", "Test Body"
          )
      ))
    );
    request.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            new NotificationRecipient()
              .setType(NotificationRecipientType.USER)
              .setId("urn:li:corpuser:test")
        )
    ));

    manager.handle(request).join();

    // Verify that "send" was called on the sink.
    Mockito.verify(notificationSink, Mockito.times(1)).send(Mockito.eq(request), Mockito.any());
  }

  @Test
  public void testHandleEnabledTemplateNotSupported() throws Exception {
    NotificationSink notificationSink = Mockito.mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(Collections.emptyList()); // Sink doesn't support any templates!
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager = new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.ENABLED,
        ImmutableList.of(notificationSink)
    );

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(new NotificationMessage()
        .setTemplate(NotificationTemplateType.CUSTOM.toString())
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title", "Test Title",
                "body", "Test Body"
            )
        ))
    );
    request.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            new NotificationRecipient()
                .setType(NotificationRecipientType.USER)
                .setId("urn:li:corpuser:test")
        )
    ));

    manager.handle(request).join();

    // Verify that "send" was NOT called on the sink. (It cannot handle the template)
    Mockito.verify(notificationSink, Mockito.times(0)).send(Mockito.same(request), Mockito.any());
  }

  @Test
  public void testHandleEnabledUnknownTemplate() {
    NotificationSink notificationSink = Mockito.mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(Collections.emptyList());
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager = new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.ENABLED,
        ImmutableList.of(notificationSink)
    );

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(new NotificationMessage()
        .setTemplate("InvalidTemplate")
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title", "Test Title",
                "body", "Test Body"
            )
        ))
    );
    request.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            new NotificationRecipient()
                .setType(NotificationRecipientType.USER)
                .setId("urn:li:corpuser:test")
        )
    ));

    Assert.assertThrows(RuntimeException.class, () -> manager.handle(request));
  }

  @Test
  public void testHandleEnabledMissingRequiredTemplateParams() {
    NotificationSink notificationSink = Mockito.mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(ImmutableList.of(NotificationTemplateType.CUSTOM));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    NotificationSinkManager manager = new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.ENABLED,
        ImmutableList.of(notificationSink)
    );

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(new NotificationMessage()
        .setTemplate("InvalidTemplate")
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title", "Test Title"
                // "body", "Test Body" // Missing required "body" parameter for template type CUSTOM
            )
        ))
    );
    request.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            new NotificationRecipient()
                .setType(NotificationRecipientType.USER)
                .setId("urn:li:corpuser:test")
        )
    ));

    Assert.assertThrows(RuntimeException.class, () -> manager.handle(request));
  }

  @Test
  public void testHandleManagerDisabled() throws Exception {
    NotificationSink notificationSink = Mockito.mock(NotificationSink.class);
    Mockito.when(notificationSink.templates()).thenReturn(ImmutableList.of(
        NotificationTemplateType.CUSTOM
    ));
    Mockito.when(notificationSink.type()).thenReturn(NotificationSinkType.SLACK);

    // Create manager in "disabled" state.
    NotificationSinkManager manager = new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.DISABLED,
        ImmutableList.of(notificationSink)
    );

    // Test Handle
    NotificationRequest request = new NotificationRequest();
    request.setMessage(new NotificationMessage()
        .setTemplate(NotificationTemplateType.CUSTOM.toString())
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title", "Test Title",
                "body", "Test Body"
            )
        ))
    );
    request.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            new NotificationRecipient()
                .setType(NotificationRecipientType.USER)
                .setId("urn:li:corpuser:test")
        )
    ));

    manager.handle(request);

    // Verify that "send" was NOT called on the sink.
    Mockito.verify(notificationSink, Mockito.times(0)).send(Mockito.same(request), Mockito.any());
  }
}