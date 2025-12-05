package com.linkedin.datahub.graphql.resolvers.ai;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.conversation.DataHubAiConversationInfo;
import com.linkedin.conversation.DataHubAiConversationOriginType;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDataHubAiConversationInput;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.datahub.graphql.types.ai.mappers.DataHubAiConversationMapper;
import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver for creating new DataHub AI conversations. */
@Slf4j
public class CreateDataHubAiConversationResolver
    implements DataFetcher<CompletableFuture<DataHubAiConversation>> {

  private final DataHubAiConversationService conversationService;

  public CreateDataHubAiConversationResolver(
      @Nonnull final DataHubAiConversationService conversationService) {
    this.conversationService = conversationService;
  }

  @Override
  public CompletableFuture<DataHubAiConversation> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final CreateDataHubAiConversationInput input =
        bindArgument(environment.getArgument("input"), CreateDataHubAiConversationInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Get current user
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());

            // Get origin type from input
            final DataHubAiConversationOriginType originType =
                input.getOriginType() != null
                    ? DataHubAiConversationOriginType.valueOf(input.getOriginType().toString())
                    : DataHubAiConversationOriginType.DATAHUB_UI;

            // Get context from input
            final com.linkedin.conversation.DataHubAiConversationContext conversationContext =
                input.getContext() != null ? mapContextInput(input.getContext()) : null;

            // Create conversation using service
            final Urn conversationUrn =
                conversationService.createConversation(
                    context.getOperationContext(),
                    input.getTitle(),
                    currentUserUrn,
                    originType,
                    conversationContext);

            // Get the conversation info
            final DataHubAiConversationInfo conversationInfo =
                conversationService.getConversation(context.getOperationContext(), conversationUrn);

            // Map to GraphQL object
            final DataHubAiConversation conversation =
                DataHubAiConversationMapper.map(
                    context, conversationInfo, conversationUrn.toString());

            log.info("Created conversation {} for user {}", conversationUrn, currentUserUrn);
            return conversation;

          } catch (Exception e) {
            log.error("Failed to create agent conversation", e);
            throw new RuntimeException("Failed to create agent conversation", e);
          }
        });
  }

  /**
   * Maps GraphQL DataHubAiConversationContextInput to PDL DataHubAiConversationContext.
   *
   * @param input the GraphQL input
   * @return the PDL DataHubAiConversationContext
   */
  private com.linkedin.conversation.DataHubAiConversationContext mapContextInput(
      com.linkedin.datahub.graphql.generated.DataHubAiConversationContextInput input) {
    final com.linkedin.conversation.DataHubAiConversationContext context =
        new com.linkedin.conversation.DataHubAiConversationContext();
    context.setText(input.getText());

    if (input.getEntityUrns() != null && !input.getEntityUrns().isEmpty()) {
      try {
        final com.linkedin.common.UrnArray entityUrns = new com.linkedin.common.UrnArray();
        for (String urnString : input.getEntityUrns()) {
          entityUrns.add(Urn.createFromString(urnString));
        }
        context.setEntityUrns(entityUrns);
      } catch (Exception e) {
        log.warn("Failed to parse entityUrns: {}", input.getEntityUrns(), e);
      }
    }

    return context;
  }
}
