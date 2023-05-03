package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.EntitySubscriptionSummary;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetEntitySubscriptionSummaryInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class GetEntitySubscriptionSummaryResolver implements DataFetcher<CompletableFuture<EntitySubscriptionSummary>> {
  private static final int DEFAULT_NUM_MAX_SUBSCRIPTIONS = 100;
  private static final int DEFAULT_NUM_GROUPS = 5;
  private final SubscriptionService _subscriptionService;
  private final GroupService _groupService;

  @Override
  public CompletableFuture<EntitySubscriptionSummary> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final GetEntitySubscriptionSummaryInput input =
        bindArgument(environment.getArgument("input"), GetEntitySubscriptionSummaryInput.class);
    final String entityUrnString = input.getEntityUrn();
    final Integer numMaxSubscriptions =
        input.getNumMaxSubscriptions() == null ? DEFAULT_NUM_MAX_SUBSCRIPTIONS : input.getNumMaxSubscriptions();
    final Integer numTopGroups = input.getNumTopGroups() == null ? DEFAULT_NUM_GROUPS : input.getNumTopGroups();
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Urn entityUrn = UrnUtils.getUrn(entityUrnString);
        final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

        final EntitySubscriptionSummary summary = new EntitySubscriptionSummary();
        summary.setIsUserSubscribed(_subscriptionService.isUserSubscribed(entityUrn, actorUrn, authentication));

        final List<Urn> userGroupUrns = _groupService.getGroupsForUser(actorUrn, authentication);
        summary.setIsUserSubscribedViaGroup(
            _subscriptionService.isAnyGroupSubscribed(entityUrn, userGroupUrns, authentication));

        summary.setNumUserSubscriptions(
            _subscriptionService.getNumUserSubscriptionsForEntity(entityUrn, numMaxSubscriptions, authentication));

        // Maxes out at 100 groups.
        summary.setNumGroupSubscriptions(
            _subscriptionService.getNumGroupSubscriptionsForEntity(entityUrn, numMaxSubscriptions, authentication));

        final List<Urn> topGroupUrns =
            _subscriptionService.getGroupSubscribersForEntity(entityUrn, numTopGroups, authentication);
        final List<CorpGroup> topGroups = topGroupUrns.stream()
            .map(urn -> {
              final CorpGroup group = new CorpGroup();
              group.setUrn(urn.toString());
              group.setType(EntityType.CORP_GROUP);
              return group;
            }).collect(Collectors.toList());
        summary.setTopGroups(topGroups);

        return summary;
      } catch (Exception e) {
        throw new RuntimeException("Failed to get subscription summary", e);
      }
    });
  }
}
