package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateSubscriptionInput;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CreateSubscriptionResolver
    implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateSubscriptionInput input =
        bindArgument(environment.getArgument("input"), CreateSubscriptionInput.class);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final String entityUrnString = input.getEntityUrn();
            final SubscriptionTypeArray subscriptionTypes =
                mapSubscriptionTypes(input.getSubscriptionTypes());
            final EntityChangeDetailsArray entityChangeTypes =
                mapEntityChangeDetails(input.getEntityChangeTypes());
            final SubscriptionNotificationConfig notificationConfig =
                input.getNotificationConfig() == null
                    ? null
                    : mapSubscriptionNotificationConfig(input.getNotificationConfig());
            final String groupUrnString = input.getGroupUrn();
            final String userUrnString = input.getUserUrn();

            if (groupUrnString != null && !canManageGroupSubscriptions(groupUrnString, context)) {
              throw new DataHubGraphQLException(
                  String.format("Unauthorized to create subscription for group %s", groupUrnString),
                  DataHubGraphQLErrorCode.UNAUTHORIZED);
            }

            if (userUrnString != null
                && !userUrnString.equals(context.getActorUrn())
                && !canManageUserSubscriptions(context)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Unauthorized to create subscription for user %s, missing MANAGE_USER_SUBSCRIPTIONS privilege",
                      userUrnString),
                  DataHubGraphQLErrorCode.UNAUTHORIZED);
            }

            // The subscription actor is the user who created the subscription, or the group if
            // nonnull or the explicit user urn if provided.

            final Urn actorUrn;
            if (userUrnString != null) {
              actorUrn = UrnUtils.getUrn(userUrnString);
            } else if (groupUrnString != null) {
              actorUrn = UrnUtils.getUrn(groupUrnString);
            } else {
              actorUrn = UrnUtils.getUrn(context.getActorUrn());
            }

            final Map.Entry<Urn, SubscriptionInfo> subscription =
                _subscriptionService.createSubscription(
                    context.getOperationContext(),
                    actorUrn,
                    UrnUtils.getUrn(entityUrnString),
                    subscriptionTypes,
                    entityChangeTypes,
                    notificationConfig);

            return DataHubSubscriptionMapper.map(context, subscription);
          } catch (DataHubGraphQLException e) { // Allow DataHub Exceptions to propagate up
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Failed to create subscriptions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
