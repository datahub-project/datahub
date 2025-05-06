package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.SubscriptionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GetEntitySubscriptionSummaryResolver
    implements DataFetcher<CompletableFuture<EntitySubscriptionSummary>> {
  private static final int DEFAULT_SUBSCRIPTION_COUNT = 100;
  private static final int DEFAULT_USER_COUNT = 50;
  private static final int DEFAULT_GROUP_COUNT = 5;
  private final SubscriptionService _subscriptionService;
  private final GroupService _groupService;

  @Override
  public CompletableFuture<EntitySubscriptionSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final GetEntitySubscriptionSummaryInput input =
        bindArgument(environment.getArgument("input"), GetEntitySubscriptionSummaryInput.class);
    final String entityUrnString = input.getEntityUrn();
    final Integer numMaxSubscriptions =
        input.getSubscriptionCount() == null
            ? DEFAULT_SUBSCRIPTION_COUNT
            : input.getSubscriptionCount();
    final Integer numExampleGroups =
        input.getNumExampleGroups() == null ? DEFAULT_GROUP_COUNT : input.getNumExampleGroups();
    final Integer numSubscribedUsers =
        input.getNumSubscribedUsers() == null ? DEFAULT_USER_COUNT : input.getNumSubscribedUsers();
    final Integer numSubscribedGroups =
        input.getNumSubscribedGroups() == null
            ? DEFAULT_GROUP_COUNT
            : input.getNumSubscribedGroups();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn entityUrn = UrnUtils.getUrn(entityUrnString);
            final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

            final EntitySubscriptionSummary summary = new EntitySubscriptionSummary();
            summary.setIsUserSubscribed(
                _subscriptionService.isActorSubscribed(
                    context.getOperationContext(), entityUrn, actorUrn));

            final List<Urn> userGroupUrns =
                _groupService.getGroupsForUser(context.getOperationContext(), actorUrn);
            summary.setIsUserSubscribedViaGroup(
                _subscriptionService.isAnyGroupSubscribed(
                    context.getOperationContext(), entityUrn, userGroupUrns));

            summary.setUserSubscriptionCount(
                _subscriptionService.getNumUserSubscriptionsForEntity(
                    context.getOperationContext(), entityUrn, numMaxSubscriptions));

            summary.setGroupSubscriptionCount(
                _subscriptionService.getNumGroupSubscriptionsForEntity(
                    context.getOperationContext(), entityUrn, numMaxSubscriptions));

            final List<Urn> subscribedGroupUrns =
                _subscriptionService.getGroupSubscribersForEntity(
                    context.getOperationContext(), entityUrn, numSubscribedGroups);

            final List<Urn> exampleGroupUrns =
                _subscriptionService.getGroupSubscribersForEntity(
                    context.getOperationContext(), entityUrn, numExampleGroups);

            final List<Urn> subscribedUsersUrns =
                _subscriptionService.getSubscribedUsersForEntity(
                    context.getOperationContext(), entityUrn, numSubscribedUsers);

            final List<CorpGroup> subscribedGroups =
                subscribedGroupUrns.stream()
                    .map(
                        urn -> {
                          final CorpGroup group = new CorpGroup();
                          group.setUrn(urn.toString());
                          group.setType(EntityType.CORP_GROUP);
                          return group;
                        })
                    .collect(Collectors.toList());
            summary.setSubscribedGroups(subscribedGroups);

            final List<CorpGroup> exampleGroups =
                exampleGroupUrns.stream()
                    .map(
                        urn -> {
                          final CorpGroup group = new CorpGroup();
                          group.setUrn(urn.toString());
                          group.setType(EntityType.CORP_GROUP);
                          return group;
                        })
                    .collect(Collectors.toList());
            summary.setExampleGroups(exampleGroups);

            final List<CorpUser> subscribedUsers =
                subscribedUsersUrns.stream()
                    .map(
                        urn -> {
                          final CorpUser user = new CorpUser();
                          user.setUrn(urn.toString());
                          user.setType(EntityType.CORP_USER);
                          return user;
                        })
                    .collect(Collectors.toList());
            summary.setSubscribedUsers(subscribedUsers);

            return summary;
          } catch (Exception e) {
            throw new RuntimeException("Failed to get subscription summary", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
