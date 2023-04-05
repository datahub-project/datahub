package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.UpdateSubscriptionInput;
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
import static com.linkedin.metadata.Constants.*;


@RequiredArgsConstructor
public class UpdateSubscriptionResolver implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final UpdateSubscriptionInput input = bindArgument(environment.getArgument("input"), UpdateSubscriptionInput.class);
    return CompletableFuture.supplyAsync(() -> {
      try {
        final SubscriptionTypeArray subscriptionTypes =
            input.getSubscriptionTypes() == null ? null : mapSubscriptionTypes(input.getSubscriptionTypes());
        final EntityChangeTypeArray entityChangeTypes =
            input.getEntityChangeTypes() == null ? null : mapEntityChangeTypes(input.getEntityChangeTypes());
        final SubscriptionNotificationConfig notificationConfig =
            input.getNotificationConfig() == null ? null
                : mapSubscriptionNotificationConfig(input.getNotificationConfig());
        final String subscriptionUrnString = input.getSubscriptionUrn();
        final Urn subscriptionUrn = UrnUtils.getUrn(subscriptionUrnString);

        final SubscriptionInfo subscriptionInfo =
            _subscriptionService.getSubscriptionInfo(subscriptionUrn, authentication);
        final Urn actorUrn = subscriptionInfo.getActorUrn();
        if (actorUrn.getEntityType().equals(CORP_GROUP_ENTITY_NAME) && !canManageGroupSubscriptions(actorUrn.toString(),
            context)) {
          throw new RuntimeException(
              String.format("Unauthorized to update subscription for group %s", actorUrn));
        }

        final Map.Entry<Urn, SubscriptionInfo> subscription = _subscriptionService.updateSubscription(
            subscriptionUrn,
            subscriptionInfo,
            subscriptionTypes,
            entityChangeTypes,
            notificationConfig,
            authentication);

        return DataHubSubscriptionMapper.map(subscription);
      } catch (Exception e) {
        throw new RuntimeException("Failed to update subscriptions", e);
      }
    });
  }
}
