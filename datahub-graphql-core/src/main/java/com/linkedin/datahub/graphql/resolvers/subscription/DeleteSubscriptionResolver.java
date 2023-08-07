package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeleteSubscriptionInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


@RequiredArgsConstructor
public class DeleteSubscriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final SubscriptionService _subscriptionService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final DeleteSubscriptionInput input = bindArgument(environment.getArgument("input"), DeleteSubscriptionInput.class);
    final String subscriptionUrnString = input.getSubscriptionUrn();
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Urn subscriptionUrn = UrnUtils.getUrn(subscriptionUrnString);

        final SubscriptionInfo subscriptionInfo =
            _subscriptionService.getSubscriptionInfo(subscriptionUrn, authentication);
        final Urn actorUrn = subscriptionInfo.getActorUrn();
        if (actorUrn.getEntityType().equals(CORP_GROUP_ENTITY_NAME) && !canManageGroupSubscriptions(actorUrn.toString(),
            context)) {
          throw new RuntimeException(
              String.format("Unauthorized to delete subscription for group %s", actorUrn));
        }
        if (actorUrn.getEntityType().equals(CORP_USER_ENTITY_NAME) && !actorUrn.toString().equals(context.getActorUrn())) {
          throw new RuntimeException(
              String.format(
                  "Unauthorized to delete personal subscription for actor user is not logged in as. User: %s, Subscription actor: %S",
                  context.getActorUrn(),
                  actorUrn
              ));
        }

        _entityClient.deleteEntity(subscriptionUrn, authentication);

        return true;
      } catch (Exception e) {
        throw new RuntimeException("Failed to delete subscription", e);
      }
    });
  }
}
