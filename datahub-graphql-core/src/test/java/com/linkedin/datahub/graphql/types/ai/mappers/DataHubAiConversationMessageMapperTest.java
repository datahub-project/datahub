package com.linkedin.datahub.graphql.types.ai.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.conversation.DataHubAiConversationActor;
import com.linkedin.conversation.DataHubAiConversationActorType;
import com.linkedin.conversation.DataHubAiConversationMessage;
import com.linkedin.conversation.DataHubAiConversationMessageContent;
import com.linkedin.conversation.DataHubAiConversationMessageType;
import org.testng.annotations.Test;

public class DataHubAiConversationMessageMapperTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String TEST_AGENT_URN = "urn:li:corpuser:datahub-agent";
  private static final long TEST_TIME = 1695456789000L;

  @Test
  public void testMapCompleteMessage() {
    // Create a complete message with all fields
    DataHubAiConversationMessage message = createCompleteMessage();

    // Map the message
    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);

    // Verify all fields are mapped correctly
    assertNotNull(result);
    assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT);
    assertEquals(result.getTime().longValue(), TEST_TIME);

    // Verify actor
    assertNotNull(result.getActor());
    assertEquals(
        result.getActor().getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.USER);
    assertEquals(result.getActor().getActor(), TEST_USER_URN);

    // Verify content
    assertNotNull(result.getContent());
    assertEquals(result.getContent().getText(), "Hello, AI!");
  }

  @Test
  public void testMapMinimalMessage() {
    // Create a minimal message with only required fields
    DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setType(DataHubAiConversationMessageType.TEXT);
    message.setTime(TEST_TIME);

    // Map the message
    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);

    // Verify basic fields
    assertNotNull(result);
    assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT);
    assertEquals(result.getTime().longValue(), TEST_TIME);

    // Optional fields should be null
    assertNull(result.getActor());
    assertNull(result.getContent());
  }

  @Test
  public void testMapMessageWithAllMessageTypes() {
    // Test all message types
    DataHubAiConversationMessageType[] pegasusTypes = {
      DataHubAiConversationMessageType.TEXT,
      DataHubAiConversationMessageType.TOOL_CALL,
      DataHubAiConversationMessageType.TOOL_RESULT,
      DataHubAiConversationMessageType.THINKING
    };

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType[] expectedTypes = {
      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT,
      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_CALL,
      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_RESULT,
      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.THINKING
    };

    for (int i = 0; i < pegasusTypes.length; i++) {
      DataHubAiConversationMessage message = new DataHubAiConversationMessage();
      message.setType(pegasusTypes[i]);
      message.setTime(TEST_TIME);

      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
          DataHubAiConversationMessageMapper.map(null, message);
      assertEquals(result.getType(), expectedTypes[i]);
    }
  }

  @Test
  public void testMapMessageWithUnknownMessageType() {
    // Test with a message that doesn't have a type set
    DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setTime(TEST_TIME);
    // No type set

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);
    assertNull(result.getType());
  }

  @Test
  public void testMapMessageWithActor() {
    DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setType(DataHubAiConversationMessageType.TEXT);
    message.setTime(TEST_TIME);

    // Set actor
    DataHubAiConversationActor actor = new DataHubAiConversationActor();
    actor.setType(DataHubAiConversationActorType.AGENT);
    actor.setActor(UrnUtils.getUrn(TEST_AGENT_URN));
    message.setActor(actor);

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);

    assertNotNull(result.getActor());
    assertEquals(
        result.getActor().getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.AGENT);
    assertEquals(result.getActor().getActor(), TEST_AGENT_URN);
  }

  @Test
  public void testMapMessageWithActorTypes() {
    // Test both actor types
    DataHubAiConversationActorType[] pegasusTypes = {
      DataHubAiConversationActorType.USER, DataHubAiConversationActorType.AGENT
    };

    com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType[] expectedTypes = {
      com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.USER,
      com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.AGENT
    };

    for (int i = 0; i < pegasusTypes.length; i++) {
      DataHubAiConversationMessage message = new DataHubAiConversationMessage();
      message.setType(DataHubAiConversationMessageType.TEXT);
      message.setTime(TEST_TIME);

      DataHubAiConversationActor actor = new DataHubAiConversationActor();
      actor.setType(pegasusTypes[i]);
      actor.setActor(UrnUtils.getUrn(TEST_USER_URN));
      message.setActor(actor);

      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
          DataHubAiConversationMessageMapper.map(null, message);
      assertEquals(result.getActor().getType(), expectedTypes[i]);
    }
  }

  @Test
  public void testMapMessageWithContent() {
    DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setType(DataHubAiConversationMessageType.TEXT);
    message.setTime(TEST_TIME);

    // Set content
    DataHubAiConversationMessageContent content = new DataHubAiConversationMessageContent();
    content.setText("Test message content");
    message.setContent(content);

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);

    assertNotNull(result.getContent());
    assertEquals(result.getContent().getText(), "Test message content");
  }

  @Test
  public void testMapMessageWithNullContext() {
    // Test that null context is handled properly
    DataHubAiConversationMessage message = createCompleteMessage();

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);
    assertNotNull(result);
  }

  @Test
  public void testMapMessageWithNonNullContext() {
    // Test that non-null context is handled properly
    DataHubAiConversationMessage message = createCompleteMessage();

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);
    assertNotNull(result);
  }

  @Test
  public void testStaticMapMethod() {
    // Test the static map method
    DataHubAiConversationMessage message = createCompleteMessage();

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.map(null, message);
    assertNotNull(result);
  }

  @Test
  public void testMapperInstance() {
    // Test that INSTANCE is not null
    assertNotNull(DataHubAiConversationMessageMapper.INSTANCE);
  }

  @Test
  public void testApplyMethod() {
    // Test the apply method directly
    DataHubAiConversationMessage message = createCompleteMessage();

    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        DataHubAiConversationMessageMapper.INSTANCE.apply(null, message);
    assertNotNull(result);
  }

  private DataHubAiConversationMessage createCompleteMessage() {
    DataHubAiConversationMessage message = new DataHubAiConversationMessage();
    message.setType(DataHubAiConversationMessageType.TEXT);
    message.setTime(TEST_TIME);

    // Set actor
    DataHubAiConversationActor actor = new DataHubAiConversationActor();
    actor.setType(DataHubAiConversationActorType.USER);
    actor.setActor(UrnUtils.getUrn(TEST_USER_URN));
    message.setActor(actor);

    // Set content
    DataHubAiConversationMessageContent content = new DataHubAiConversationMessageContent();
    content.setText("Hello, AI!");

    message.setContent(content);
    return message;
  }
}
