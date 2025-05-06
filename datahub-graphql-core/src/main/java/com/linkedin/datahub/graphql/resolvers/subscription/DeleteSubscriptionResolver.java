package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.DeleteSubscriptionInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DeleteSubscriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final SubscriptionService _subscriptionService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final DeleteSubscriptionInput input =
        bindArgument(environment.getArgument("input"), DeleteSubscriptionInput.class);
    final String subscriptionUrnString = input.getSubscriptionUrn();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn subscriptionUrn = UrnUtils.getUrn(subscriptionUrnString);

            final SubscriptionInfo subscriptionInfo =
                _subscriptionService.getSubscriptionInfo(
                    context.getOperationContext(), subscriptionUrn);
            final Urn actorUrn = subscriptionInfo.getActorUrn();
            if (actorUrn.getEntityType().equals(CORP_GROUP_ENTITY_NAME)
                && !canManageGroupSubscriptions(actorUrn.toString(), context)) {
              throw new DataHubGraphQLException(
                  String.format("Unauthorized to delete subscription for group %s", actorUrn),
                  DataHubGraphQLErrorCode.UNAUTHORIZED);
            }
            if (actorUrn.getEntityType().equals(CORP_USER_ENTITY_NAME)
                && !actorUrn.toString().equals(context.getActorUrn())
                && !canManageUserSubscriptions(context)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Unauthorized to delete subscription for user %s, missing MANAGE_USER_SUBSCRIPTIONS privilege",
                      actorUrn),
                  DataHubGraphQLErrorCode.UNAUTHORIZED);
            }

            _entityClient.deleteEntity(context.getOperationContext(), subscriptionUrn);

            return true;
          } catch (DataHubGraphQLException e) { // Allow DataHub Exceptions to propagate up
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Failed to delete subscription", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
