package com.linkedin.datahub.graphql.resolvers.ai;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubAiConversation;
import com.linkedin.datahub.graphql.generated.DataHubAiConversationConnection;
import com.linkedin.datahub.graphql.generated.DataHubAiConversationOriginType;
import com.linkedin.datahub.graphql.types.ai.mappers.DataHubAiConversationMapper;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.DataHubAiConversationService;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver for listing DataHub AI conversations for the current user. */
@Slf4j
public class ListDataHubAiConversationsResolver
    implements DataFetcher<CompletableFuture<DataHubAiConversationConnection>> {

  private final DataHubAiConversationService conversationService;

  public ListDataHubAiConversationsResolver(
      @Nonnull final DataHubAiConversationService conversationService) {
    this.conversationService = conversationService;
  }

  @Override
  public CompletableFuture<DataHubAiConversationConnection> get(
      DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Integer start = environment.getArgument("start");
    final Integer count = environment.getArgument("count");
    final DataHubAiConversationOriginType originType = environment.getArgument("originType");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Get current user
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());

            // Build filter for originType if provided
            Filter originTypeFilter = null;
            if (originType != null) {
              final Criterion criterion =
                  CriterionUtils.buildCriterion(
                      "originType", Condition.EQUAL, originType.toString());
              originTypeFilter =
                  new Filter()
                      .setOr(
                          new ConjunctiveCriterionArray(
                              new ConjunctiveCriterion().setAnd(new CriterionArray(criterion))));
            }

            // List conversations using service
            final DataHubAiConversationService.ConversationListResult listResult =
                conversationService.listConversations(
                    context.getOperationContext(),
                    currentUserUrn,
                    count != null ? count : 20,
                    start != null ? start : 0,
                    originTypeFilter);

            // Map to GraphQL objects
            final List<DataHubAiConversation> conversations =
                listResult.getConversations().stream()
                    .map(
                        result ->
                            DataHubAiConversationMapper.map(
                                context, result.getInfo(), result.getUrn().toString()))
                    .collect(Collectors.toList());

            // Create connection
            final DataHubAiConversationConnection connection =
                new DataHubAiConversationConnection();
            connection.setConversations(conversations);
            connection.setTotal(listResult.getTotalCount());

            return connection;

          } catch (Exception e) {
            log.error("Failed to list agent conversations", e);
            throw new RuntimeException("Failed to list agent conversations", e);
          }
        });
  }

  private DataHubAiConversationConnection createEmptyConnection() {
    final DataHubAiConversationConnection connection = new DataHubAiConversationConnection();
    connection.setConversations(List.of());
    connection.setTotal(0);
    return connection;
  }
}
