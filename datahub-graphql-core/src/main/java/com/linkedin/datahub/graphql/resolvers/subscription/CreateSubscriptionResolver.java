package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateSubscriptionInput;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionResolverUtils.*;


@RequiredArgsConstructor
public class CreateSubscriptionResolver implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final CreateSubscriptionInput input = bindArgument(environment.getArgument("input"), CreateSubscriptionInput.class);
    return CompletableFuture.supplyAsync(() -> {
      try {
        final String entityUrnString = input.getEntityUrn();
        final SubscriptionTypeArray subscriptionTypes = mapSubscriptionTypes(input.getSubscriptionTypes());
        final EntityChangeTypeArray entityChangeTypes = mapEntityChangeTypes(input.getEntityChangeTypes());
        final SubscriptionNotificationConfig notificationConfig =
            mapSubscriptionNotificationConfig(input.getNotificationConfig());
        final String groupUrnString = input.getGroupUrn();

        if (groupUrnString != null && !canManageGroupSubscriptions(groupUrnString, context)) {
          throw new RuntimeException(
              String.format("Unauthorized to create subscription for group %s", groupUrnString));
        }

        // The subscription actor is the user who created the subscription, or the group if nonnull.
        final Urn actorUrn =
            groupUrnString == null ? UrnUtils.getUrn(context.getActorUrn()) : UrnUtils.getUrn(groupUrnString);

        final Map.Entry<Urn, SubscriptionInfo> subscription = _subscriptionService.createSubscription(
            actorUrn,
            UrnUtils.getUrn(entityUrnString),
            subscriptionTypes,
            entityChangeTypes,
            notificationConfig,
            authentication);

        return DataHubSubscriptionMapper.map(subscription);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create subscriptions", e);
      }
    });
  }
}
