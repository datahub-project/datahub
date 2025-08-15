package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.SyncSubscriptionInput;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SyncSubscriptionResolver
    implements DataFetcher<CompletableFuture<DataHubSubscription>> {
  private final SubscriptionService _subscriptionService;

  @Override
  public CompletableFuture<DataHubSubscription> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final SyncSubscriptionInput input =
        bindArgument(environment.getArgument("input"), SyncSubscriptionInput.class);

    // Extract input parameters
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final Urn actorUrn = UrnUtils.getUrn(input.getActorUrn());

    // Validate that the actor URN is either a group or user
    final String actorEntityType = actorUrn.getEntityType();
    if (!CORP_GROUP_ENTITY_NAME.equals(actorEntityType)
        && !CORP_USER_ENTITY_NAME.equals(actorEntityType)) {
      throw new DataHubGraphQLException(
          String.format(
              "Invalid actor URN: expected group or user URN, found entity type %s",
              actorEntityType),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    final EntityChangeDetailsArray newEntityChangeTypes =
        input.getEntityChangeTypes() == null
            ? new EntityChangeDetailsArray()
            : mapEntityChangeDetails(input.getEntityChangeTypes());
    final SubscriptionNotificationConfig notificationConfig =
        input.getNotificationConfig() == null
            ? null
            : mapSubscriptionNotificationConfig(input.getNotificationConfig());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Authorization
            validateSyncSubscriptionAuthorization(context, actorUrn, actorEntityType);

            // Check if subscription already exists
            final Map.Entry<Urn, SubscriptionInfo> existingSubscription =
                _subscriptionService.getSubscription(
                    context.getOperationContext(), entityUrn, actorUrn);

            final Map.Entry<Urn, SubscriptionInfo> result;
            if (existingSubscription != null) {
              // Update existing subscription by merging entity change types
              final Urn subscriptionUrn = existingSubscription.getKey();
              final SubscriptionInfo existingInfo = existingSubscription.getValue();

              final EntityChangeDetailsArray mergedEntityChangeTypes =
                  mergeEntityChangeDetails(
                      existingInfo.getEntityChangeTypes(), newEntityChangeTypes);

              result =
                  _subscriptionService.updateSubscription(
                      context.getOperationContext(),
                      actorUrn,
                      subscriptionUrn,
                      existingInfo,
                      /* subscriptionTypes */ null,
                      mergedEntityChangeTypes,
                      notificationConfig);
            } else {
              // Create new subscription with default ENTITY_CHANGE type
              final SubscriptionTypeArray subscriptionTypes =
                  new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE);
              result =
                  _subscriptionService.createSubscription(
                      context.getOperationContext(),
                      actorUrn,
                      entityUrn,
                      subscriptionTypes,
                      newEntityChangeTypes,
                      notificationConfig);
            }

            return DataHubSubscriptionMapper.map(context, result);
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Failed to sync subscription", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private EntityChangeDetailsArray mergeEntityChangeDetails(
      @Nonnull EntityChangeDetailsArray existing, @Nonnull EntityChangeDetailsArray incoming) {
    final Map<com.linkedin.subscription.EntityChangeType, EntityChangeDetails> typeToDetails =
        new LinkedHashMap<>();

    // Add all existing change types
    for (EntityChangeDetails detail : existing) {
      typeToDetails.put(detail.getEntityChangeType(), detail);
    }

    // Add all incoming change types (overwrite existing ones)
    for (EntityChangeDetails detail : incoming) {
      typeToDetails.put(detail.getEntityChangeType(), detail);
    }

    final EntityChangeDetailsArray mergedArray = new EntityChangeDetailsArray();
    for (EntityChangeDetails value : typeToDetails.values()) {
      mergedArray.add(value);
    }
    return mergedArray;
  }

  private void validateSyncSubscriptionAuthorization(
      QueryContext context, Urn actorUrn, String actorEntityType) {
    if (CORP_GROUP_ENTITY_NAME.equals(actorEntityType)) {
      if (!canManageGroupSubscriptions(actorUrn.toString(), context)) {
        throw new DataHubGraphQLException(
            String.format("Unauthorized to sync subscription for group %s", actorUrn),
            DataHubGraphQLErrorCode.UNAUTHORIZED);
      }
    } else { // USER
      if (!actorUrn.toString().equals(context.getActorUrn())
          || !canManageUserSubscriptions(context)) {
        throw new DataHubGraphQLException(
            String.format(
                "Unauthorized to sync subscription for user %s, missing MANAGE_USER_SUBSCRIPTIONS privilege",
                actorUrn),
            DataHubGraphQLErrorCode.UNAUTHORIZED);
      }
    }
  }
}
