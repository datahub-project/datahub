package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.ListSubscriptionsInput;
import com.linkedin.datahub.graphql.generated.ListSubscriptionsResult;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class ListSubscriptionsResolver implements DataFetcher<CompletableFuture<ListSubscriptionsResult>> {
  final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<ListSubscriptionsResult> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final ListSubscriptionsInput input = bindArgument(environment.getArgument("input"), ListSubscriptionsInput.class);
    final String actorUrnString = input.getActorUrn();
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Urn actorUrn = UrnUtils.getUrn(actorUrnString);
        final Map<Urn, SubscriptionInfo> subscriptions =
            _subscriptionService.listSubscriptions(actorUrn, authentication);

        final List<DataHubSubscription> dataHubSubscriptions = subscriptions
            .entrySet().stream()
            .map(DataHubSubscriptionMapper::map)
            .collect(Collectors.toList());
        final ListSubscriptionsResult result = new ListSubscriptionsResult();
        result.setSubscriptions(dataHubSubscriptions);

        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list subscriptions", e);
      }
    });
  }
}
