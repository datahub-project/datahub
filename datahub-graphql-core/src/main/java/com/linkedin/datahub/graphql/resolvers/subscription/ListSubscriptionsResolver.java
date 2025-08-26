package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.ListSubscriptionsInput;
import com.linkedin.datahub.graphql.generated.ListSubscriptionsResult;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.subscription.SubscriptionInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ListSubscriptionsResolver
    implements DataFetcher<CompletableFuture<ListSubscriptionsResult>> {
  final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<ListSubscriptionsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListSubscriptionsInput input =
        bindArgument(environment.getArgument("input"), ListSubscriptionsInput.class);
    final int start = input.getStart() == null ? 0 : input.getStart();
    final int count = input.getCount() == null ? 10 : input.getCount();
    final String groupUrnString = input.getGroupUrn();
    final String actorUrnString =
        Stream.of(groupUrnString, input.getActorUrn(), context.getActorUrn())
            .filter(Objects::nonNull)
            .findFirst()
            .get();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (groupUrnString != null && !canManageGroupSubscriptions(groupUrnString, context)) {
              throw new RuntimeException(
                  String.format("Unauthorized to list subscriptions for group %s", groupUrnString));
            }

            final Urn actorUrn = UrnUtils.getUrn(actorUrnString);
            final SearchResult searchResult =
                _subscriptionService.getSubscriptionsSearchResult(
                    context.getOperationContext(), actorUrn, start, count);
            final Map<Urn, SubscriptionInfo> subscriptions =
                _subscriptionService.listSubscriptions(context.getOperationContext(), searchResult);

            final List<DataHubSubscription> dataHubSubscriptions =
                subscriptions.entrySet().stream()
                    .map(s -> DataHubSubscriptionMapper.map(context, s))
                    .collect(Collectors.toList());
            final ListSubscriptionsResult result = new ListSubscriptionsResult();
            result.setSubscriptions(dataHubSubscriptions);
            result.setTotal(searchResult.getNumEntities());

            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list subscriptions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
