package com.linkedin.datahub.graphql.resolvers.notification;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.FormNotificationRecipientInput;
import com.linkedin.datahub.graphql.generated.FormNotificationType;
import com.linkedin.datahub.graphql.generated.NotificationRecipientType;
import com.linkedin.datahub.graphql.generated.SendFormNotificationRequestInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.mxe.PlatformEvent;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class FormNotificationRequestUtilsTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_RECIPIENT_ID = "test-recipient";

  @Test
  public void testMapNotificationRequestInputWithAllFields() {
    // Create test input
    SendFormNotificationRequestInput input = createTestInput();

    // Map the input
    NotificationRequest result =
        FormNotificationRequestUtils.mapFormNotificationRequestInput(input);

    // Verify message mapping
    assertEquals(
        result.getMessage().getTemplate(),
        com.linkedin.event.notification.template.NotificationTemplateType
            .BROADCAST_COMPLIANCE_FORM_PUBLISH);
    assertEquals(result.getMessage().getParameters().get("key"), "value");

    // Verify recipient mapping
    assertEquals(result.getRecipients().size(), 1);
    assertEquals(
        result.getRecipients().get(0).getType().toString(),
        NotificationRecipientType.EMAIL.toString());
    assertEquals(result.getRecipients().get(0).getId(), TEST_RECIPIENT_ID);
    assertEquals(result.getRecipients().get(0).getActor().toString(), TEST_ACTOR_URN);
    assertEquals(result.getRecipients().get(0).getDisplayName(), "Test User");
  }

  @Test
  public void testMapNotificationRequestInputWithMinimalFields() {
    // Create minimal input
    SendFormNotificationRequestInput input = new SendFormNotificationRequestInput();
    input.setType(FormNotificationType.BROADCAST_COMPLIANCE_FORM_PUBLISH);
    input.setParameters(new ArrayList<>());

    // Create minimal recipient input
    List<FormNotificationRecipientInput> recipients = new ArrayList<>();
    FormNotificationRecipientInput recipient = new FormNotificationRecipientInput();
    recipient.setType(NotificationRecipientType.EMAIL);
    recipient.setId(TEST_RECIPIENT_ID);
    recipients.add(recipient);
    input.setRecipients(recipients);

    // Map the input
    NotificationRequest result =
        FormNotificationRequestUtils.mapFormNotificationRequestInput(input);

    // Verify message mapping
    assertEquals(
        result.getMessage().getTemplate(),
        com.linkedin.event.notification.template.NotificationTemplateType
            .BROADCAST_COMPLIANCE_FORM_PUBLISH);
    assertTrue(result.getMessage().getParameters().isEmpty());

    // Verify recipient mapping
    assertEquals(result.getRecipients().size(), 1);
    assertEquals(
        result.getRecipients().get(0).getType().toString(),
        NotificationRecipientType.EMAIL.toString());
    assertEquals(result.getRecipients().get(0).getId(), TEST_RECIPIENT_ID);
    assertNull(result.getRecipients().get(0).getOrigin());
    assertNull(result.getRecipients().get(0).getActor());
    assertNull(result.getRecipients().get(0).getDisplayName());
    assertNull(result.getRecipients().get(0).getCustomType());
  }

  @Test
  public void testCreatePlatformEvent() {
    // Create test input and map it
    SendFormNotificationRequestInput input = createTestInput();
    NotificationRequest notificationRequest =
        FormNotificationRequestUtils.mapFormNotificationRequestInput(input);

    // Create platform event
    PlatformEvent platformEvent =
        FormNotificationRequestUtils.createPlatformEvent(notificationRequest);

    // Verify platform event
    assertNotNull(platformEvent);
    assertNotNull(platformEvent.getName());
    assertNotNull(platformEvent.getPayload());
    assertNotNull(platformEvent.getHeader());
    assertTrue(platformEvent.getHeader().getTimestampMillis() > 0);
  }

  private SendFormNotificationRequestInput createTestInput() {
    SendFormNotificationRequestInput input = new SendFormNotificationRequestInput();

    input.setType(FormNotificationType.BROADCAST_COMPLIANCE_FORM_PUBLISH);
    List<StringMapEntryInput> parameters = new ArrayList<>();
    StringMapEntryInput param = new StringMapEntryInput();
    param.setKey("key");
    param.setValue("value");
    parameters.add(param);
    input.setParameters(parameters);

    // Create recipient input
    List<FormNotificationRecipientInput> recipients = new ArrayList<>();
    FormNotificationRecipientInput recipient = new FormNotificationRecipientInput();
    recipient.setType(NotificationRecipientType.EMAIL);
    recipient.setId(TEST_RECIPIENT_ID);
    recipient.setActor(TEST_ACTOR_URN);
    recipient.setDisplayName("Test User");
    recipients.add(recipient);
    input.setRecipients(recipients);

    return input;
  }
}
