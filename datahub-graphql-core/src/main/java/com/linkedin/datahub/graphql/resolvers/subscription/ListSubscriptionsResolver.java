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
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class ListSubscriptionsResolver implements DataFetcher<CompletableFuture<ListSubscriptionsResult>> {
  final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<ListSubscriptionsResult> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final ListSubscriptionsInput input = bindArgument(environment.getArgument("input"), ListSubscriptionsInput.class);
    final int start = input.getStart() == null ? 0 : input.getStart();
    final int count = input.getCount() == null ? 10 : input.getCount();
    final String groupUrnString = input.getGroupUrn();
    final String actorUrnString = groupUrnString == null ? context.getActorUrn() : groupUrnString;
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (groupUrnString != null && !canManageGroupSubscriptions(groupUrnString, context)) {
          throw new RuntimeException(
              String.format("Unauthorized to list subscriptions for group %s", groupUrnString));
        }

        final Urn actorUrn = UrnUtils.getUrn(actorUrnString);
        final SearchResult searchResult = _subscriptionService.getSubscriptionsSearchResult(actorUrn, start, count, authentication);
        final Map<Urn, SubscriptionInfo> subscriptions = _subscriptionService.listSubscriptions(searchResult, authentication);

        final List<DataHubSubscription> dataHubSubscriptions = subscriptions
            .entrySet().stream()
            .map(DataHubSubscriptionMapper::map)
            .collect(Collectors.toList());
        final ListSubscriptionsResult result = new ListSubscriptionsResult();
        result.setSubscriptions(dataHubSubscriptions);
        result.setTotal(searchResult.getNumEntities());

        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list subscriptions", e);
      }
    });
  }
}
