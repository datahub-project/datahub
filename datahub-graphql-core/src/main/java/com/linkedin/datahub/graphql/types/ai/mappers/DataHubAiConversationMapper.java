package com.linkedin.datahub.graphql.types.ai.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.datahub.graphql.generated.DataHubAiConversationMessage;
import com.linkedin.datahub.graphql.generated.DataHubAiConversationOriginType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link DataHubAiConversationInfo} objects to GraphQL {@link DataHubAiConversation}
 * objects.
 */
public class DataHubAiConversationMapper
    implements ModelMapper<DataHubAiConversationInfo, DataHubAiConversation> {

  public static final DataHubAiConversationMapper INSTANCE = new DataHubAiConversationMapper();

  public static DataHubAiConversation map(
      @Nullable QueryContext context,
      @Nonnull final DataHubAiConversationInfo conversationInfo,
      @Nonnull final String conversationUrn) {
    return INSTANCE.apply(context, conversationInfo, conversationUrn);
  }

  @Override
  public DataHubAiConversation apply(
      @Nullable QueryContext context, @Nonnull final DataHubAiConversationInfo conversationInfo) {
    throw new UnsupportedOperationException(
        "Use apply(context, conversationInfo, conversationUrn) instead");
  }

  public DataHubAiConversation apply(
      @Nullable QueryContext context,
      @Nonnull final DataHubAiConversationInfo conversationInfo,
      @Nonnull final String conversationUrn) {

    final DataHubAiConversation result = new DataHubAiConversation();
    result.setUrn(conversationUrn);

    // Map title
    if (conversationInfo.hasTitle()) {
      result.setTitle(conversationInfo.getTitle());
    }

    // Map origin type
    result.setOriginType(
        DataHubAiConversationOriginType.valueOf(conversationInfo.getOriginType().toString()));

    if (conversationInfo.hasContext()) {
      final com.linkedin.conversation.DataHubAiConversationContext pdlContext =
          conversationInfo.getContext();
      final com.linkedin.datahub.graphql.generated.DataHubAiConversationContext graphqlContext =
          new com.linkedin.datahub.graphql.generated.DataHubAiConversationContext();
      graphqlContext.setText(pdlContext.getText());
      if (pdlContext.hasEntityUrns()) {
        graphqlContext.setEntityUrns(
            pdlContext.getEntityUrns().stream().map(Object::toString).collect(Collectors.toList()));
      }
      result.setContext(graphqlContext);
    }

    // Map basic fields
    if (conversationInfo.hasCreated()) {
      final AuditStamp created = conversationInfo.getCreated();
      result.setCreated(
          com.linkedin.datahub.graphql.generated.AuditStamp.builder()
              .setTime(created.getTime())
              .setActor(created.getActor().toString())
              .build());
    }

    // Map messages
    if (conversationInfo.hasMessages()) {
      final List<DataHubAiConversationMessage> messages =
          conversationInfo.getMessages().stream()
              .map(message -> DataHubAiConversationMessageMapper.map(context, message))
              .collect(Collectors.toList());
      result.setMessages(messages);
    } else {
      result.setMessages(List.of());
    }

    result.setMessageCount(result.getMessages().size());

    // Set last updated to created time for now (could be enhanced later)
    if (conversationInfo.hasCreated()) {
      result.setLastUpdated(
          com.linkedin.datahub.graphql.generated.AuditStamp.builder()
              .setTime(conversationInfo.getCreated().getTime())
              .setActor(conversationInfo.getCreated().getActor().toString())
              .build());
    }

    return result;
  }
}
