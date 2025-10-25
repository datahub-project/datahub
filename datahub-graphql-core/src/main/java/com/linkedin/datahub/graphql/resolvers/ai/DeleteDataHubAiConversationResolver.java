package com.linkedin.datahub.graphql.resolvers.ai;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver for deleting DataHub AI conversations. */
@Slf4j
public class DeleteDataHubAiConversationResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final DataHubAiConversationService conversationService;

  public DeleteDataHubAiConversationResolver(
      @Nonnull final DataHubAiConversationService conversationService) {
    this.conversationService = conversationService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = environment.getArgument("urn");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Validate URN
            if (urn == null || urn.trim().isEmpty()) {
              throw new IllegalArgumentException("Conversation URN is required");
            }

            final Urn conversationUrn = Urn.createFromString(urn);

            // Verify this is a conversation URN
            if (!AcrylConstants.AGENT_CONVERSATION_ENTITY_NAME.equals(
                conversationUrn.getEntityType())) {
              throw new IllegalArgumentException("Invalid conversation URN: " + urn);
            }

            // Get current user for authorization check
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());

            // Check if user can access this conversation
            if (!conversationService.canAccessConversation(
                context.getOperationContext(), conversationUrn, currentUserUrn)) {
              throw new RuntimeException(
                  "User does not have permission to delete this conversation");
            }

            // Delete the conversation using service
            conversationService.deleteConversation(context.getOperationContext(), conversationUrn);

            log.info("Deleted conversation {} by user {}", urn, currentUserUrn);
            return true;

          } catch (Exception e) {
            log.error("Failed to delete agent conversation {}", urn, e);
            throw new RuntimeException("Failed to delete agent conversation", e);
          }
        });
  }
}
