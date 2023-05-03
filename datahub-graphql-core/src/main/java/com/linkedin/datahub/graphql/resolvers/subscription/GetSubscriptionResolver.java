package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.GetSubscriptionInput;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class GetSubscriptionResolver implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final GetSubscriptionInput input = bindArgument(environment.getArgument("input"), GetSubscriptionInput.class);
    final String entityUrnString = input.getEntityUrn();
    final String groupUrnString = input.getGroupUrn();
    final String actorUrnString = groupUrnString == null ? context.getActorUrn() : groupUrnString;
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (groupUrnString != null && !canManageGroupSubscriptions(groupUrnString, context)) {
          throw new RuntimeException(
              String.format("Unauthorized to get subscription for group %s", groupUrnString));
        }

        final Urn entityUrn = UrnUtils.getUrn(entityUrnString);
        final Urn actorUrn = UrnUtils.getUrn(actorUrnString);

        final Map.Entry<Urn, SubscriptionInfo> subscription =
            _subscriptionService.getSubscription(entityUrn, actorUrn, authentication);

        return subscription == null ? null : DataHubSubscriptionMapper.map(subscription);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to get subscription for actor %s and entity %s", actorUrnString, entityUrnString), e);
      }
    });
  }
}
