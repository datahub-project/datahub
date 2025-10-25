package com.linkedin.datahub.graphql.resolvers.ai;

import com.linkedin.common.urn.Urn;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.datahub.graphql.types.ai.mappers.DataHubAiConversationMapper;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver for getting a specific DataHub AI conversation by URN. */
@Slf4j
public class GetDataHubAiConversationResolver
    implements DataFetcher<CompletableFuture<DataHubAiConversation>> {

  private final DataHubAiConversationService conversationService;

  public GetDataHubAiConversationResolver(
      @Nonnull final DataHubAiConversationService conversationService) {
    this.conversationService = conversationService;
  }

  @Override
  public CompletableFuture<DataHubAiConversation> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String conversationUrn = environment.getArgument("urn");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Validate URN format
            final Urn urn;
            try {
              urn = Urn.createFromString(conversationUrn);
            } catch (Exception e) {
              log.warn("Invalid conversation URN format: {}", conversationUrn, e);
              return null;
            }

            // Verify this is a dataHubConversation URN
            if (!AcrylConstants.AGENT_CONVERSATION_ENTITY_NAME.equals(urn.getEntityType())) {
              log.warn("URN is not a dataHubConversation: {}", conversationUrn);
              return null;
            }

            // Get current user for authorization
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());

            // Check if user can access this conversation
            if (!conversationService.canAccessConversation(
                context.getOperationContext(), urn, currentUserUrn)) {
              log.warn(
                  "User {} does not have access to conversation {}",
                  currentUserUrn,
                  conversationUrn);
              return null;
            }

            // Fetch the conversation using service
            final DataHubAiConversationInfo conversationInfo =
                conversationService.getConversation(context.getOperationContext(), urn);

            if (conversationInfo == null) {
              log.warn("Conversation not found: {}", conversationUrn);
              return null;
            }

            // Map to GraphQL object
            final DataHubAiConversation conversation =
                DataHubAiConversationMapper.map(context, conversationInfo, conversationUrn);

            log.info("Retrieved conversation {} for user {}", conversationUrn);
            return conversation;

          } catch (Exception e) {
            log.error("Failed to get agent conversation: {}", conversationUrn, e);
            throw new RuntimeException("Failed to get agent conversation", e);
          }
        });
  }
}
