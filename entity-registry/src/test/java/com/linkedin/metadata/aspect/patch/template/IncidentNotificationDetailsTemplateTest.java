package com.linkedin.metadata.aspect.patch.template;

import static org.testng.Assert.assertEquals;

import com.linkedin.incident.IncidentNotificationDetails;
import com.linkedin.metadata.aspect.patch.template.incident.IncidentNotificationDetailsTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class IncidentNotificationDetailsTemplateTest {
  @Test
  public void testAddMessage() throws Exception {
    IncidentNotificationDetailsTemplate incidentNotificationDetailsTemplate =
        new IncidentNotificationDetailsTemplate();
    IncidentNotificationDetails baseDetails = incidentNotificationDetailsTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
    JsonObjectBuilder detailsNode = Json.createObjectBuilder();

    detailsNode.add("messageId", "test-message-id");
    detailsNode.add("channelName", "Test");
    detailsNode.add("channelId", "test-channel-id");

    jsonPatchBuilder.add("/slack/messages/test-message-id", detailsNode.build());

    // Case 1: Initial population test
    IncidentNotificationDetails result =
        incidentNotificationDetailsTemplate.applyPatch(baseDetails, jsonPatchBuilder.build());

    assertEquals(result.getSlack().getMessages().size(), 1);
    assertEquals(result.getSlack().getMessages().get(0).getMessageId(), "test-message-id");
    assertEquals(result.getSlack().getMessages().get(0).getChannelName(), "Test");
    assertEquals(result.getSlack().getMessages().get(0).getChannelId(), "test-channel-id");

    // Case 2: Test non-overwrite existing assertions using new assertion URN
    jsonPatchBuilder = Json.createPatchBuilder();
    detailsNode = Json.createObjectBuilder();

    detailsNode.add("messageId", "test-message-id-2");
    detailsNode.add("channelName", "Test 2");
    detailsNode.add("channelId", "test-channel-id-2");

    jsonPatchBuilder.add("/slack/messages/test-message-id-2", detailsNode.build());

    result = incidentNotificationDetailsTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getSlack().getMessages().size(), 2);
    assertEquals(result.getSlack().getMessages().get(1).getMessageId(), "test-message-id-2");
    assertEquals(result.getSlack().getMessages().get(1).getChannelName(), "Test 2");
    assertEquals(result.getSlack().getMessages().get(1).getChannelId(), "test-channel-id-2");

    // Case 3: Try to add the same again.
    jsonPatchBuilder = Json.createPatchBuilder();
    detailsNode = Json.createObjectBuilder();

    detailsNode.add("messageId", "test-message-id");
    detailsNode.add("channelName", "Test");
    detailsNode.add("channelId", "test-channel-id");

    jsonPatchBuilder.add("/slack/messages/test-message-id", detailsNode.build());

    result = incidentNotificationDetailsTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getSlack().getMessages().size(), 2);
    assertEquals(result.getSlack().getMessages().get(0).getMessageId(), "test-message-id");
    assertEquals(result.getSlack().getMessages().get(0).getChannelName(), "Test");
    assertEquals(result.getSlack().getMessages().get(0).getChannelId(), "test-channel-id");
    assertEquals(result.getSlack().getMessages().get(1).getMessageId(), "test-message-id-2");
    assertEquals(result.getSlack().getMessages().get(1).getChannelName(), "Test 2");
    assertEquals(result.getSlack().getMessages().get(1).getChannelId(), "test-channel-id-2");
  }
}
