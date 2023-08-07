package com.datahub.subscription;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
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
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.Collections;
import java.util.List;
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
  private static final Set<String> SUBSCRIPTION_ASPECTS = ImmutableSet.of(SUBSCRIPTION_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> createSubscription(@Nonnull final Urn actorUrn, @Nonnull final Urn entityUrn,
      @Nonnull final SubscriptionTypeArray subscriptionTypes, @Nonnull final EntityChangeDetailsArray entityChangeTypes,
      @Nullable final SubscriptionNotificationConfig notificationConfig, @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final String subscriptionID = UUID.randomUUID().toString();
      final SubscriptionKey subscriptionKey = new SubscriptionKey().setId(subscriptionID);

      final AuditStamp auditStamp = new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);

      final SubscriptionInfo subscriptionInfo = new SubscriptionInfo().setActorUrn(actorUrn)
          .setActorType(actorUrn.getEntityType())
          .setEntityUrn(entityUrn)
          .setTypes(subscriptionTypes)
          .setEntityChangeTypes(entityChangeTypes)
          .setCreatedOn(auditStamp)
          .setUpdatedOn(auditStamp);
      if (notificationConfig != null) {
        subscriptionInfo.setNotificationConfig(notificationConfig);
      }

      final MetadataChangeProposal proposal = buildMetadataChangeProposal(
          SUBSCRIPTION_ENTITY_NAME,
          subscriptionKey,
          SUBSCRIPTION_INFO_ASPECT_NAME,
          subscriptionInfo
      );
      final String subscriptionUrnString = _entityClient.ingestProposal(proposal, authentication, false);
      final Urn subscriptionUrn = Urn.createFromString(subscriptionUrnString);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create subscription for actor %s on entity %s", actorUrn, entityUrn), e);
    }
  }

  public boolean isUserSubscribed(@Nonnull final Urn entityUrn, @Nonnull final Urn actorUrn,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      final Filter filter = buildGetSubscriptionFilter(entityUrn, actorUrn);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          1,
          authentication
      );

      return searchResult.hasEntities() && !searchResult.getEntities().isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get subscription for actor %s and entity %s", actorUrn, entityUrn),
          e);
    }
  }

  public boolean isAnyGroupSubscribed(@Nonnull final Urn entityUrn, @Nonnull final List<Urn> groupUrns,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      for (Urn groupUrn : groupUrns) {
        if (!_entityClient.exists(groupUrn, authentication)) {
          throw new RuntimeException(String.format("Group %s does not exist", groupUrn));
        }
      }

      final Filter filter = buildIsAnyGroupSubscribedFilter(entityUrn, groupUrns);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          1,
          authentication
      );

      return searchResult.hasEntities() && !searchResult.getEntities().isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get subscription for groups %s and entity %s", groupUrns, entityUrn),
          e);
    }
  }

  @Nullable
  public Map.Entry<Urn, SubscriptionInfo> getSubscription(@Nonnull final Urn entityUrn, @Nonnull final Urn actorUrn,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      final Filter filter = buildGetSubscriptionFilter(entityUrn, actorUrn);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          1,
          authentication
      );

      if (!searchResult.hasEntities() || searchResult.getEntities().isEmpty()) {
        return null;
      }

      final Urn subscriptionUrn = searchResult.getEntities().get(0).getEntity();

      final SubscriptionInfo subscriptionInfo = getSubscriptionInfo(subscriptionUrn, authentication);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get subscription for actor %s and entity %s", actorUrn, entityUrn),
          e);
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
  public Map.Entry<Urn, SubscriptionInfo> updateSubscription(@Nonnull final Urn actorUrn,
      @Nonnull final Urn subscriptionUrn,
      @Nonnull final SubscriptionInfo subscriptionInfo,
      @Nullable final SubscriptionTypeArray subscriptionTypes, @Nullable final EntityChangeDetailsArray entityChangeTypes,
      @Nullable final SubscriptionNotificationConfig notificationConfig, @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(subscriptionUrn, authentication)) {
        throw new RuntimeException(String.format("Subscription %s does not exist", subscriptionUrn));
      }

      final AuditStamp auditStamp = new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);
      subscriptionInfo.setUpdatedOn(auditStamp);

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
      _entityClient.ingestProposal(proposal, authentication, false);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update subscription %s", subscriptionUrn), e);
    }
  }

  @Nonnull
  public SearchResult getSubscriptionsSearchResult(
      @Nonnull final Urn actorUrn,
      final int start,
      final int count,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(actorUrn, authentication)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }
      final Filter filter = buildListSubscriptionsFilter(actorUrn);
      return _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          start,
          count,
          authentication
      );

    } catch (Exception e) {
      throw new RuntimeException("Failed to search for subscriptions", e);
    }
  }

  @Nonnull
  public Map<Urn, SubscriptionInfo> listSubscriptions(
      @Nonnull final SearchResult searchResult,
      @Nonnull final Authentication authentication) {
    try {
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

  public int getNumUserSubscriptionsForEntity(@Nonnull final Urn entityUrn, @Nonnull final Integer numMaxSubscriptions,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter = buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_USER_ENTITY_NAME);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          numMaxSubscriptions,
          authentication
      );

      return searchResult.hasEntities() ? searchResult.getEntities().size() : 0;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get number of subscriptions for entity %s", entityUrn), e);
    }
  }

  public int getNumGroupSubscriptionsForEntity(@Nonnull final Urn entityUrn, @Nonnull final Integer numMaxSubscriptions,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter = buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_GROUP_ENTITY_NAME);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          numMaxSubscriptions,
          authentication
      );

      return searchResult.hasEntities() ? searchResult.getEntities().size() : 0;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get number of subscriptions for entity %s", entityUrn), e);
    }
  }

  @Nonnull
  public List<Urn> getGroupSubscribersForEntity(@Nonnull final Urn entityUrn, @Nonnull final Integer numTopGroups,
      @Nonnull final Authentication authentication) {
    try {
      if (!_entityClient.exists(entityUrn, authentication)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter = buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_GROUP_ENTITY_NAME);
      final SearchResult searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          numTopGroups,
          authentication
      );

      final Set<Urn> subscriptionUrns = searchResult.getEntities()
          .stream()
          .map(SearchEntity::getEntity)
          .collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entityResponseMap = _entityClient.batchGetV2(
          SUBSCRIPTION_ENTITY_NAME,
          subscriptionUrns,
          SUBSCRIPTION_ASPECTS,
          authentication
      );

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.values()
          .stream()
          .map(SubscriptionInfo::new)
          .map(SubscriptionInfo::getActorUrn)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get top subscriptions for entity %s", entityUrn), e);
    }
  }

  @Nonnull
  private Filter buildGetSubscriptionFilter(
      @Nonnull final Urn entityUrn, @Nonnull final Urn actorUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    andCriterion.add(buildEntityCriterion(entityUrn));
    andCriterion.add(buildActorCriterion(actorUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private Filter buildIsAnyGroupSubscribedFilter(
      @Nonnull final Urn entityUrn, @Nonnull final List<Urn> groupUrns) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    for (Urn groupUrn : groupUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      andCriterion.add(buildEntityCriterion(entityUrn));
      andCriterion.add(buildActorCriterion(groupUrn));
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }

    filter.setOr(disjunction);

    return filter;
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
  private Filter buildGetActorSubscriptionsForEntityFilter(@Nonnull final Urn entityUrn,
      @Nonnull final String actorType) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    andCriterion.add(buildEntityCriterion(entityUrn));
    andCriterion.add(buildActorTypeCriterion(actorType));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private Criterion buildEntityCriterion(
      @Nonnull final Urn entityUrn) {
    final Criterion entityCriterion = new Criterion();
    entityCriterion.setField(ENTITY_URN_FIELD_NAME);
    entityCriterion.setValue(entityUrn.toString());
    entityCriterion.setCondition(Condition.EQUAL);
    return entityCriterion;
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

  @Nonnull
  private Criterion buildActorTypeCriterion(
      @Nonnull final String actorType) {
    final Criterion actorTypeCriterion = new Criterion();
    actorTypeCriterion.setField(ACTOR_TYPE_FIELD_NAME);
    actorTypeCriterion.setValue(actorType);
    actorTypeCriterion.setCondition(Condition.EQUAL);
    return actorTypeCriterion;
  }
}
