package com.linkedin.datahub.graphql.types.ai.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.conversation.DataHubAiConversationActor;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.conversation.DataHubAiConversationMessageArray;
import com.linkedin.conversation.DataHubAiConversationMessageContent;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import org.testng.annotations.Test;

public class DataHubAiConversationMapperTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String TEST_CONVERSATION_URN = "urn:li:dataHubAiConversation:testConv123";
  private static final long TEST_TIME = 1695456789000L;

  @Test
  public void testMapCompleteConversation() throws Exception {
    // Create a complete conversation info with all fields populated
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    // Set created audit stamp
    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Set origin type
    conversationInfo.setOriginType(
        com.linkedin.conversation.DataHubAiConversationOriginType.DATAHUB_UI);

    // Create messages
    DataHubAiConversationMessageArray messages = new DataHubAiConversationMessageArray();

    // Add a user message
    com.linkedin.conversation.DataHubAiConversationMessage userMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    userMessage.setType(com.linkedin.conversation.DataHubAiConversationMessageType.TEXT);
    userMessage.setTime(TEST_TIME);

    DataHubAiConversationActor userActor = new DataHubAiConversationActor();
    userActor.setType(com.linkedin.conversation.DataHubAiConversationActorType.USER);
    userActor.setActor(UrnUtils.getUrn(TEST_USER_URN));
    userMessage.setActor(userActor);

    DataHubAiConversationMessageContent userContent = new DataHubAiConversationMessageContent();
    userContent.setText("Hello, what datasets do we have?");
    userMessage.setContent(userContent);

    messages.add(userMessage);

    // Add an agent message
    com.linkedin.conversation.DataHubAiConversationMessage agentMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    agentMessage.setType(com.linkedin.conversation.DataHubAiConversationMessageType.TEXT);
    agentMessage.setTime(TEST_TIME + 1000);

    DataHubAiConversationActor agentActor = new DataHubAiConversationActor();
    agentActor.setType(com.linkedin.conversation.DataHubAiConversationActorType.AGENT);
    agentActor.setActor(UrnUtils.getUrn("urn:li:corpuser:datahub-agent"));
    agentMessage.setActor(agentActor);

    DataHubAiConversationMessageContent agentContent = new DataHubAiConversationMessageContent();
    agentContent.setText("I found 5 datasets for you.");
    agentMessage.setContent(agentContent);

    messages.add(agentMessage);

    conversationInfo.setMessages(messages);

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify basic fields
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN);

    // Verify origin type
    assertNotNull(result.getOriginType());
    assertEquals(
        result.getOriginType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationOriginType.DATAHUB_UI);

    // Verify created audit stamp
    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getTime().longValue(), TEST_TIME);
    assertEquals(result.getCreated().getActor(), TEST_USER_URN);

    // Verify lastUpdated (should be same as created for now)
    assertNotNull(result.getLastUpdated());
    assertEquals(result.getLastUpdated().getTime().longValue(), TEST_TIME);
    assertEquals(result.getLastUpdated().getActor(), TEST_USER_URN);

    // Verify messages
    assertNotNull(result.getMessages());
    assertEquals(result.getMessages().size(), 2);

    // Verify first message (user message)
    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage firstMessage =
        result.getMessages().get(0);
    assertEquals(
        firstMessage.getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT);
    assertEquals(firstMessage.getTime().longValue(), TEST_TIME);
    assertEquals(
        firstMessage.getActor().getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.USER);
    assertEquals(firstMessage.getActor().getActor(), TEST_USER_URN);
    assertEquals(firstMessage.getContent().getText(), "Hello, what datasets do we have?");

    // Verify second message (agent message)
    com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage secondMessage =
        result.getMessages().get(1);
    assertEquals(
        secondMessage.getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT);
    assertEquals(secondMessage.getTime().longValue(), TEST_TIME + 1000);
    assertEquals(
        secondMessage.getActor().getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.AGENT);
    assertEquals(secondMessage.getContent().getText(), "I found 5 datasets for you.");
  }

  @Test
  public void testMapMinimalConversation() throws Exception {
    // Create conversation with minimal fields
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    // Only set created audit stamp
    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify basic fields
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN);

    // Verify messages are empty list
    assertNotNull(result.getMessages());
    assertEquals(result.getMessages().size(), 0);

    // Verify created and user are set
    assertNotNull(result.getCreated());
  }

  @Test
  public void testMapConversationWithoutCreated() {
    // Create conversation without created field
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify basic fields
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN);

    // Verify created fields are null
    assertNull(result.getCreated());
    assertNull(result.getLastUpdated());

    // Verify messages are empty
    assertNotNull(result.getMessages());
    assertEquals(result.getMessages().size(), 0);
  }

  @Test
  public void testMapConversationWithEmptyMessages() throws Exception {
    // Create conversation with empty messages array
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Set empty messages array
    conversationInfo.setMessages(new DataHubAiConversationMessageArray());

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify messages are empty
    assertNotNull(result.getMessages());
    assertEquals(result.getMessages().size(), 0);
  }

  @Test
  public void testMapConversationWithMultipleMessages() throws Exception {
    // Create conversation with multiple messages
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Create multiple messages
    DataHubAiConversationMessageArray messages = new DataHubAiConversationMessageArray();

    for (int i = 0; i < 5; i++) {
      com.linkedin.conversation.DataHubAiConversationMessage message =
          new com.linkedin.conversation.DataHubAiConversationMessage();
      message.setType(com.linkedin.conversation.DataHubAiConversationMessageType.TEXT);
      message.setTime(TEST_TIME + (i * 1000));

      DataHubAiConversationActor actor = new DataHubAiConversationActor();
      actor.setType(
          i % 2 == 0
              ? com.linkedin.conversation.DataHubAiConversationActorType.USER
              : com.linkedin.conversation.DataHubAiConversationActorType.AGENT);
      actor.setActor(UrnUtils.getUrn(i % 2 == 0 ? TEST_USER_URN : "urn:li:corpuser:datahub-agent"));
      message.setActor(actor);

      DataHubAiConversationMessageContent content = new DataHubAiConversationMessageContent();
      content.setText("Message " + i);
      message.setContent(content);

      messages.add(message);
    }

    conversationInfo.setMessages(messages);

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify messages
    assertNotNull(result.getMessages());
    assertEquals(result.getMessages().size(), 5);

    // Verify each message is mapped
    for (int i = 0; i < 5; i++) {
      com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage message =
          result.getMessages().get(i);
      assertNotNull(message);
      assertEquals(message.getTime().longValue(), TEST_TIME + (i * 1000));
      assertEquals(message.getContent().getText(), "Message " + i);
    }
  }

  @Test
  public void testMapConversationWithDifferentMessageTypes() throws Exception {
    // Create conversation with different message types
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    DataHubAiConversationMessageArray messages = new DataHubAiConversationMessageArray();

    // TEXT message
    com.linkedin.conversation.DataHubAiConversationMessage textMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    textMessage.setType(com.linkedin.conversation.DataHubAiConversationMessageType.TEXT);
    textMessage.setTime(TEST_TIME);
    DataHubAiConversationMessageContent textContent = new DataHubAiConversationMessageContent();
    textContent.setText("Text message");
    textMessage.setContent(textContent);
    messages.add(textMessage);

    // TOOL_CALL message
    com.linkedin.conversation.DataHubAiConversationMessage toolCallMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    toolCallMessage.setType(com.linkedin.conversation.DataHubAiConversationMessageType.TOOL_CALL);
    toolCallMessage.setTime(TEST_TIME + 1000);
    DataHubAiConversationMessageContent toolCallContent = new DataHubAiConversationMessageContent();
    toolCallContent.setText("Tool call message");
    toolCallMessage.setContent(toolCallContent);
    messages.add(toolCallMessage);

    // TOOL_RESULT message
    com.linkedin.conversation.DataHubAiConversationMessage toolResultMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    toolResultMessage.setType(
        com.linkedin.conversation.DataHubAiConversationMessageType.TOOL_RESULT);
    toolResultMessage.setTime(TEST_TIME + 2000);
    DataHubAiConversationMessageContent toolResultContent =
        new DataHubAiConversationMessageContent();
    toolResultContent.setText("Tool result message");
    toolResultMessage.setContent(toolResultContent);
    messages.add(toolResultMessage);

    // THINKING message
    com.linkedin.conversation.DataHubAiConversationMessage thinkingMessage =
        new com.linkedin.conversation.DataHubAiConversationMessage();
    thinkingMessage.setType(com.linkedin.conversation.DataHubAiConversationMessageType.THINKING);
    thinkingMessage.setTime(TEST_TIME + 3000);
    DataHubAiConversationMessageContent thinkingContent = new DataHubAiConversationMessageContent();
    thinkingContent.setText("Thinking message");
    thinkingMessage.setContent(thinkingContent);
    messages.add(thinkingMessage);

    conversationInfo.setMessages(messages);

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify all message types are mapped correctly
    assertEquals(result.getMessages().size(), 4);
    assertEquals(
        result.getMessages().get(0).getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT);
    assertEquals(
        result.getMessages().get(1).getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_CALL);
    assertEquals(
        result.getMessages().get(2).getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_RESULT);
    assertEquals(
        result.getMessages().get(3).getType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.THINKING);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testApplyWithoutConversationUrnThrowsException() throws Exception {
    // Test that calling apply without conversationUrn throws an exception
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // This should throw UnsupportedOperationException
    DataHubAiConversationMapper.INSTANCE.apply(null, conversationInfo);
  }

  @Test
  public void testMapStaticMethod() throws Exception {
    // Test the static map method
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Use static map method
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_CONVERSATION_URN);
  }

  @Test
  public void testMapperInstance() {
    // Test that INSTANCE is not null
    assertNotNull(DataHubAiConversationMapper.INSTANCE);
  }

  @Test
  public void testMapConversationWithOriginType() throws Exception {
    // Create conversation with origin type
    DataHubAiConversationInfo conversationInfo = new DataHubAiConversationInfo();

    AuditStamp created = new AuditStamp();
    created.setTime(TEST_TIME);
    created.setActor(UrnUtils.getUrn(TEST_USER_URN));
    conversationInfo.setCreated(created);

    // Set origin type
    conversationInfo.setOriginType(
        com.linkedin.conversation.DataHubAiConversationOriginType.DATAHUB_UI);

    // Map the conversation
    DataHubAiConversation result =
        DataHubAiConversationMapper.map(null, conversationInfo, TEST_CONVERSATION_URN);

    // Verify origin type is mapped correctly
    assertNotNull(result);
    assertNotNull(result.getOriginType());
    assertEquals(
        result.getOriginType(),
        com.linkedin.datahub.graphql.generated.DataHubAiConversationOriginType.DATAHUB_UI);
  }
}
