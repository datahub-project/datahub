package com.datahub.subscription;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.ActorType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.SubscriptionKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
@RequiredArgsConstructor
public class SubscriptionService {
  private static final String ACTOR_URN_FIELD_NAME = "actorUrn";
  private static final Set<String> SUBSCRIPTION_ASPECTS = ImmutableSet.of(SUBSCRIPTION_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> createSubscription(@Nonnull final Urn actorUrn, @Nonnull final Urn entityUrn,
      @Nonnull final SubscriptionTypeArray subscriptionTypes, @Nonnull final EntityChangeTypeArray entityChangeTypes,
      @Nonnull final SubscriptionNotificationConfig notificationConfig, @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final String entityType = actorUrn.getEntityType();
      ActorType actorType;
      switch (entityType) {
        case CORP_USER_ENTITY_NAME:
          actorType = ActorType.USER;
          break;
        case CORP_GROUP_ENTITY_NAME:
          actorType = ActorType.GROUP;
          break;
        default:
          throw new RuntimeException(String.format("Invalid actor type %s", entityType));
      }

      final String subscriptionID = UUID.randomUUID().toString();
      final SubscriptionKey subscriptionKey = new SubscriptionKey().setId(subscriptionID);

      final SubscriptionInfo subscriptionInfo = new SubscriptionInfo().setActorUrn(actorUrn)
          .setEntityUrn(entityUrn)
          .setActorType(actorType)
          .setTypes(subscriptionTypes)
          .setEntityChangeTypes(entityChangeTypes)
          .setNotificationConfig(notificationConfig);
      subscriptionInfo.setActorUrn(actorUrn);

      final MetadataChangeProposal proposal = buildMetadataChangeProposal(
          SUBSCRIPTION_ENTITY_NAME,
          subscriptionKey,
          SUBSCRIPTION_INFO_ASPECT_NAME,
          subscriptionInfo
      );
      final String subscriptionUrnString = _entityClient.ingestProposal(proposal, authentication, true);
      final Urn subscriptionUrn = Urn.createFromString(subscriptionUrnString);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create subscription for actor %s on entity %s", actorUrn, entityUrn), e);
    }
  }

  @Nonnull
  public SubscriptionInfo getSubscriptionInfo(@Nonnull final Urn subscriptionUrn,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(subscriptionUrn, authentication)) {
        throw new RuntimeException(String.format("Subscription %s does not exist", subscriptionUrn));
      }

      final EntityResponse entityResponse = _entityClient.getV2(
          SUBSCRIPTION_ENTITY_NAME,
          subscriptionUrn,
          SUBSCRIPTION_ASPECTS,
          authentication
      );

      if (entityResponse == null) {
        throw new RuntimeException(String.format("Subscription %s does not exist", subscriptionUrn));
      }

      return new SubscriptionInfo(getDataMapFromEntityResponse(entityResponse, SUBSCRIPTION_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get subscription info for subscription %s", subscriptionUrn),
          e);
    }
  }

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> updateSubscription(@Nonnull final Urn subscriptionUrn,
      @Nonnull final SubscriptionInfo subscriptionInfo,
      @Nullable final SubscriptionTypeArray subscriptionTypes, @Nullable final EntityChangeTypeArray entityChangeTypes,
      @Nullable final SubscriptionNotificationConfig notificationConfig, @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(subscriptionUrn, authentication)) {
        throw new RuntimeException(String.format("Subscription %s does not exist", subscriptionUrn));
      }

      if (subscriptionTypes != null) {
        subscriptionInfo.setTypes(subscriptionTypes);
      }

      if (entityChangeTypes != null) {
        subscriptionInfo.setEntityChangeTypes(entityChangeTypes);
      }

      if (notificationConfig != null) {
        subscriptionInfo.setNotificationConfig(notificationConfig);
      }

      final MetadataChangeProposal proposal = buildMetadataChangeProposal(
          subscriptionUrn,
          SUBSCRIPTION_INFO_ASPECT_NAME,
          subscriptionInfo
      );
      _entityClient.ingestProposal(proposal, authentication, true);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update subscription %s", subscriptionUrn), e);
    }
  }

  @Nonnull
  public Map<Urn, SubscriptionInfo> listSubscriptions(
      @Nonnull final Urn actorUrn,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      final Filter filter = buildListSubscriptionsFilter(actorUrn);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          1000,
          authentication
      );

      final Set<Urn> subscriptionUrns = searchResult.getEntities()
          .stream()
          .map(SearchEntity::getEntity)
          .collect(Collectors.toSet());

      if (subscriptionUrns.isEmpty()) {
        return Collections.emptyMap();
      }

      final Map<Urn, EntityResponse> entityResponseMap = _entityClient.batchGetV2(
          SUBSCRIPTION_ENTITY_NAME,
          subscriptionUrns,
          SUBSCRIPTION_ASPECTS,
          authentication
      );

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> new SubscriptionInfo(entry.getValue())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to list subscriptions", e);
    }
  }

  @Nonnull
  private Filter buildListSubscriptionsFilter(
      @Nonnull final Urn actorUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    andCriterion.add(buildActorCriterion(actorUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private Criterion buildActorCriterion(
      @Nonnull final Urn actorUrn) {
    final Criterion actorCriterion = new Criterion();
    actorCriterion.setField(ACTOR_URN_FIELD_NAME);
    actorCriterion.setValue(actorUrn.toString());
    actorCriterion.setCondition(Condition.EQUAL);
    return actorCriterion;
  }
}
