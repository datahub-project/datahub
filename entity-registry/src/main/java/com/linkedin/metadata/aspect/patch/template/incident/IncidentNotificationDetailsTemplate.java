package com.linkedin.metadata.aspect.patch.template.incident;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.incident.IncidentNotificationDetails;
import com.linkedin.incident.SlackIncidentDetails;
import com.linkedin.incident.SlackMessageDetailsArray;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Patch template for {@link com.linkedin.incident.IncidentNotificationDetails} aspect. Allows
 * adding and removing assertion summary details atomically.
 */
public class IncidentNotificationDetailsTemplate
    extends CompoundKeyTemplate<IncidentNotificationDetails> {
  private static final String SLACK_MESSAGES_FIELD_PATH = "slack/messages";
  private static final String MESSAGE_ID_FIELD_NAME = "messageId";

  @Override
  public IncidentNotificationDetails getSubtype(RecordTemplate recordTemplate)
      throws ClassCastException {
    if (recordTemplate instanceof IncidentNotificationDetails) {
      return (IncidentNotificationDetails) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to IncidentNotificationDetails");
  }

  @Override
  public Class<IncidentNotificationDetails> getTemplateType() {
    return IncidentNotificationDetails.class;
  }

  @Nonnull
  @Override
  public IncidentNotificationDetails getDefault() {
    IncidentNotificationDetails details = new IncidentNotificationDetails();
    details.setSlack(new SlackIncidentDetails().setMessages(new SlackMessageDetailsArray()));
    return details;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode copyNode = baseNode.deepCopy();
    if (baseNode.has("slack")) {
      JsonNode slackNode = baseNode.get("slack");
      JsonNode transformedSlackNode =
          arrayFieldToMap(slackNode, "messages", Collections.singletonList("messageId"));
      ((ObjectNode) copyNode).set("slack", transformedSlackNode);
    }
    return copyNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    // Do opposite of above.
    JsonNode copyNode = patched.deepCopy();
    if (patched.has("slack")) {
      JsonNode slackNode = patched.get("slack");
      JsonNode transformedSlackNode =
          transformedMapToArray(slackNode, "messages", Collections.singletonList("messageId"));
      ((ObjectNode) copyNode).set("slack", transformedSlackNode);
    }
    return copyNode;
  }
}
