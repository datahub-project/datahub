package com.linkedin.datahub.graphql.resolvers.ai;

import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.idl.RuntimeWiring;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Configuration class for agent conversation resolvers. */
@Slf4j
public class DataHubAiConversationResolvers {

  private static final String QUERY_TYPE = "Query";
  private static final String MUTATION_TYPE = "Mutation";
  private static final String LIST_DATAHUB_AI_CONVERSATIONS = "listDataHubAiConversations";
  private static final String GET_DATAHUB_AI_CONVERSATION = "getDataHubAiConversation";
  private static final String CREATE_DATAHUB_AI_CONVERSATION = "createDataHubAiConversation";
  private static final String DELETE_DATAHUB_AI_CONVERSATION = "deleteDataHubAiConversation";

  private final DataHubAiConversationService conversationService;

  public DataHubAiConversationResolvers(
      @Nonnull final DataHubAiConversationService conversationService) {
    this.conversationService = conversationService;
  }

  /** Configure all agent conversation resolvers. */
  public void configureResolvers(final RuntimeWiring.Builder builder) {
    log.info("Configuring agent conversation resolvers");

    // Configure query resolvers
    builder.type(
        QUERY_TYPE,
        typeWiring ->
            typeWiring
                .dataFetcher(
                    LIST_DATAHUB_AI_CONVERSATIONS,
                    new ListDataHubAiConversationsResolver(conversationService))
                .dataFetcher(
                    GET_DATAHUB_AI_CONVERSATION,
                    new GetDataHubAiConversationResolver(conversationService)));

    // Configure mutation resolvers
    builder.type(
        MUTATION_TYPE,
        typeWiring ->
            typeWiring
                .dataFetcher(
                    CREATE_DATAHUB_AI_CONVERSATION,
                    new CreateDataHubAiConversationResolver(conversationService))
                .dataFetcher(
                    DELETE_DATAHUB_AI_CONVERSATION,
                    new DeleteDataHubAiConversationResolver(conversationService)));

    log.info("Agent conversation resolvers configured successfully");
  }
}
