package com.linkedin.datahub.graphql.types.ai.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link com.linkedin.conversation.DataHubAiConversationMessage} objects to GraphQL
 * {@link com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage} objects.
 */
public class DataHubAiConversationMessageMapper
    implements ModelMapper<
        com.linkedin.conversation.DataHubAiConversationMessage,
        com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage> {

  public static final DataHubAiConversationMessageMapper INSTANCE =
      new DataHubAiConversationMessageMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.conversation.DataHubAiConversationMessage message) {
    return INSTANCE.apply(context, message);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.conversation.DataHubAiConversationMessage message) {

    final com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage result =
        new com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage();

    // Map message type
    if (message.hasType()) {
      result.setType(mapMessageType(message.getType()));
    }

    // Map timestamp
    if (message.hasTime()) {
      result.setTime(message.getTime());
    }

    // Map actor
    if (message.hasActor()) {
      result.setActor(mapActor(message.getActor()));
    }

    // Map content
    if (message.hasContent()) {
      result.setContent(mapContent(message.getContent()));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType
      mapMessageType(com.linkedin.conversation.DataHubAiConversationMessageType type) {
    switch (type) {
      case TEXT:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT;
      case TOOL_CALL:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_CALL;
      case TOOL_RESULT:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TOOL_RESULT;
      case THINKING:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.THINKING;
      default:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageType.TEXT;
    }
  }

  private static com.linkedin.datahub.graphql.generated.DataHubAiConversationActor mapActor(
      com.linkedin.conversation.DataHubAiConversationActor actor) {
    final com.linkedin.datahub.graphql.generated.DataHubAiConversationActor result =
        new com.linkedin.datahub.graphql.generated.DataHubAiConversationActor();

    if (actor.hasType()) {
      result.setType(mapActorType(actor.getType()));
    }

    if (actor.hasActor()) {
      result.setActor(actor.getActor().toString());
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType mapActorType(
      com.linkedin.conversation.DataHubAiConversationActorType type) {
    switch (type) {
      case USER:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.USER;
      case AGENT:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.AGENT;
      default:
        return com.linkedin.datahub.graphql.generated.DataHubAiConversationActorType.USER;
    }
  }

  private static com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageContent
      mapContent(com.linkedin.conversation.DataHubAiConversationMessageContent content) {
    final com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageContent result =
        new com.linkedin.datahub.graphql.generated.DataHubAiConversationMessageContent();

    if (content.hasText()) {
      result.setText(content.getText());
    }

    return result;
  }
}
