package com.linkedin.datahub.graphql.resolvers.notification;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.FormNotificationRecipientInput;
import com.linkedin.datahub.graphql.generated.SendFormNotificationRequestInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.template.NotificationTemplateType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FormNotificationRequestUtils {

  public static PlatformEvent createPlatformEvent(final NotificationRequest request) {
    PlatformEvent event = new PlatformEvent();
    event.setName(Constants.NOTIFICATION_REQUEST_EVENT_NAME);
    event.setPayload(GenericRecordUtils.serializePayload(request));
    event.setHeader(new PlatformEventHeader().setTimestampMillis(System.currentTimeMillis()));
    return event;
  }

  /**
   * Maps a GraphQL NotificationRequestInput to a NotificationRequest
   *
   * @param input The GraphQL input to map
   * @return A NotificationRequest object
   */
  public static NotificationRequest mapFormNotificationRequestInput(
      SendFormNotificationRequestInput input) {
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(createMessageFromInput(input));
    notificationRequest.setRecipients(mapRecipientsInput(input.getRecipients()));
    return notificationRequest;
  }

  private static NotificationMessage createMessageFromInput(
      SendFormNotificationRequestInput input) {
    NotificationMessage message = new NotificationMessage();
    message.setTemplate(NotificationTemplateType.valueOf(input.getType().toString()));
    message.setParameters(mapParametersInput(input.getParameters()));
    return message;
  }

  private static StringMap mapParametersInput(List<StringMapEntryInput> parameters) {
    if (parameters == null) {
      return new StringMap();
    }
    Map<String, String> paramMap =
        parameters.stream()
            .collect(
                Collectors.toMap(
                    StringMapEntryInput::getKey,
                    StringMapEntryInput::getValue,
                    (v1, v2) -> v1 // In case of duplicate keys, keep the first value
                    ));
    return new StringMap(paramMap);
  }

  private static NotificationRecipientArray mapRecipientsInput(
      List<FormNotificationRecipientInput> recipients) {
    if (recipients == null) {
      return new NotificationRecipientArray();
    }
    List<NotificationRecipient> recipientList =
        recipients.stream()
            .map(FormNotificationRequestUtils::mapRecipientInput)
            .collect(Collectors.toList());
    return new NotificationRecipientArray(recipientList);
  }

  private static NotificationRecipient mapRecipientInput(
      FormNotificationRecipientInput recipientInput) {
    NotificationRecipient recipient = new NotificationRecipient();
    recipient.setType(NotificationRecipientType.valueOf(recipientInput.getType().toString()));

    if (recipientInput.getId() != null) {
      recipient.setId(recipientInput.getId());
    }
    if (recipientInput.getActor() != null) {
      recipient.setActor(UrnUtils.getUrn(recipientInput.getActor()));
    }
    if (recipientInput.getDisplayName() != null) {
      recipient.setDisplayName(recipientInput.getDisplayName());
    }

    return recipient;
  }
}
