package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.UpdateSubscriptionInput;
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
public class UpdateSubscriptionResolver
    implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final UpdateSubscriptionInput input =
        bindArgument(environment.getArgument("input"), UpdateSubscriptionInput.class);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final SubscriptionTypeArray subscriptionTypes =
                input.getSubscriptionTypes() == null
                    ? null
                    : mapSubscriptionTypes(input.getSubscriptionTypes());
            final EntityChangeDetailsArray entityChangeTypes =
                input.getEntityChangeTypes() == null
                    ? null
                    : mapEntityChangeDetails(input.getEntityChangeTypes());
            final SubscriptionNotificationConfig notificationConfig =
                input.getNotificationConfig() == null
                    ? null
                    : mapSubscriptionNotificationConfig(input.getNotificationConfig());
            final String subscriptionUrnString = input.getSubscriptionUrn();
            final Urn subscriptionUrn = UrnUtils.getUrn(subscriptionUrnString);

            final SubscriptionInfo subscriptionInfo =
                _subscriptionService.getSubscriptionInfo(
                    context.getOperationContext(), subscriptionUrn);
            final Urn actorUrn = subscriptionInfo.getActorUrn();
            if (actorUrn.getEntityType().equals(CORP_GROUP_ENTITY_NAME)
                && !canManageGroupSubscriptions(actorUrn.toString(), context)) {
              throw new RuntimeException(
                  String.format("Unauthorized to update subscription for group %s", actorUrn));
            }
            if (actorUrn.getEntityType().equals(CORP_USER_ENTITY_NAME)
                && !actorUrn.toString().equals(context.getActorUrn())) {
              throw new RuntimeException(
                  String.format(
                      "Unauthorized to update personal subscription for actor user is not logged in as. User: %s, Subscription actor: %S",
                      context.getActorUrn(), actorUrn));
            }

            final Map.Entry<Urn, SubscriptionInfo> subscription =
                _subscriptionService.updateSubscription(
                    context.getOperationContext(),
                    actorUrn,
                    subscriptionUrn,
                    subscriptionInfo,
                    subscriptionTypes,
                    entityChangeTypes,
                    notificationConfig);

            return DataHubSubscriptionMapper.map(context, subscription);
          } catch (Exception e) {
            throw new RuntimeException("Failed to update subscriptions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
