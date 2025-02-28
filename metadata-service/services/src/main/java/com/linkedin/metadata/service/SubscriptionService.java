package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
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
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubscriptionService extends BaseService {
  private static final Set<String> SUBSCRIPTION_ASPECTS =
      ImmutableSet.of(SUBSCRIPTION_INFO_ASPECT_NAME);

  public SubscriptionService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> createSubscription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final SubscriptionTypeArray subscriptionTypes,
      @Nonnull final EntityChangeDetailsArray entityChangeTypes,
      @Nullable final SubscriptionNotificationConfig notificationConfig) {
    try {
      if (isActorSubscribed(opContext, entityUrn, actorUrn)) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to create new subscription! Actor %s is already subscribed to entity %s",
                actorUrn, entityUrn));
      }

      final String subscriptionID = UUID.randomUUID().toString();
      final SubscriptionKey subscriptionKey = new SubscriptionKey().setId(subscriptionID);

      final AuditStamp auditStamp =
          new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);

      final SubscriptionInfo subscriptionInfo =
          new SubscriptionInfo()
              .setActorUrn(actorUrn)
              .setActorType(actorUrn.getEntityType())
              .setEntityUrn(entityUrn)
              .setTypes(subscriptionTypes)
              .setEntityChangeTypes(entityChangeTypes)
              .setCreatedOn(auditStamp)
              .setUpdatedOn(auditStamp);
      if (notificationConfig != null) {
        subscriptionInfo.setNotificationConfig(notificationConfig);
      }

      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              SUBSCRIPTION_ENTITY_NAME,
              subscriptionKey,
              SUBSCRIPTION_INFO_ASPECT_NAME,
              subscriptionInfo);
      final String subscriptionUrnString =
          this.entityClient.ingestProposal(opContext, proposal, false);
      final Urn subscriptionUrn =
          subscriptionUrnString.startsWith("urn:")
              ? Urn.createFromString(subscriptionUrnString)
              : EntityKeyUtils.convertEntityKeyToUrn(subscriptionKey, SUBSCRIPTION_ENTITY_NAME);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to create subscription for actor %s on entity %s", actorUrn, entityUrn),
          e);
    }
  }

  public boolean isActorSubscribed(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn actorUrn) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      if (!this.entityClient.exists(opContext, actorUrn, false)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      final Filter filter = buildGetSubscriptionFilter(entityUrn, actorUrn);
      final SearchResult searchResult =
          this.entityClient.filter(opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, 1);

      return searchResult.hasEntities() && !searchResult.getEntities().isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to get subscription for actor %s and entity %s", actorUrn, entityUrn),
          e);
    }
  }

  public boolean isAnyGroupSubscribed(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull List<Urn> groupUrns) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      // remove non-existent groups from the input before proceeding
      groupUrns = new LinkedList<>(groupUrns);
      Iterator<Urn> groupUrnIterator = groupUrns.iterator();

      while (groupUrnIterator.hasNext()) {
        Urn groupUrn = groupUrnIterator.next();
        if (!this.entityClient.exists(opContext, groupUrn, false)) {
          log.error("Group {} does not exist", groupUrn);
          groupUrnIterator.remove();
        }
      }

      final Filter filter = buildIsAnyGroupSubscribedFilter(entityUrn, groupUrns);
      final SearchResult searchResult =
          this.entityClient.filter(opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, 1);

      return searchResult.hasEntities() && !searchResult.getEntities().isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to get subscription for groups %s and entity %s", groupUrns, entityUrn),
          e);
    }
  }

  @Nullable
  public Map.Entry<Urn, SubscriptionInfo> getSubscription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn actorUrn) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      if (!this.entityClient.exists(opContext, actorUrn, false)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }

      final Filter filter = buildGetSubscriptionFilter(entityUrn, actorUrn);
      final SearchResult searchResult =
          this.entityClient.filter(opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, 1);

      if (!searchResult.hasEntities() || searchResult.getEntities().isEmpty()) {
        return null;
      }

      final Urn subscriptionUrn = searchResult.getEntities().get(0).getEntity();

      final SubscriptionInfo subscriptionInfo = getSubscriptionInfo(opContext, subscriptionUrn);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to get subscription for actor %s and entity %s", actorUrn, entityUrn),
          e);
    }
  }

  @Nonnull
  public SubscriptionInfo getSubscriptionInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn subscriptionUrn) {
    try {
      if (!this.entityClient.exists(opContext, subscriptionUrn)) {
        throw new RuntimeException(
            String.format("Subscription %s does not exist", subscriptionUrn));
      }

      final EntityResponse entityResponse =
          this.entityClient.getV2(
              opContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrn, SUBSCRIPTION_ASPECTS);

      if (entityResponse == null) {
        throw new RuntimeException(
            String.format("Subscription %s does not exist", subscriptionUrn));
      }

      return new SubscriptionInfo(
          getDataMapFromEntityResponse(entityResponse, SUBSCRIPTION_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get subscription info for subscription %s", subscriptionUrn), e);
    }
  }

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> updateSubscriptionInfo(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final Urn subscriptionUrn,
      @Nonnull final SubscriptionInfo subscriptionInfo) {
    try {
      if (!this.entityClient.exists(systemOpContext, subscriptionUrn)) {
        throw new RuntimeException(
            String.format("Subscription %s does not exist", subscriptionUrn));
      }

      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              subscriptionUrn, SUBSCRIPTION_INFO_ASPECT_NAME, subscriptionInfo);
      this.entityClient.ingestProposal(systemOpContext, proposal, false);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update subscription %s", subscriptionUrn), e);
    }
  }

  @Nonnull
  public Map.Entry<Urn, SubscriptionInfo> updateSubscription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn subscriptionUrn,
      @Nonnull final SubscriptionInfo subscriptionInfo,
      @Nullable final SubscriptionTypeArray subscriptionTypes,
      @Nullable final EntityChangeDetailsArray entityChangeTypes,
      @Nullable final SubscriptionNotificationConfig notificationConfig) {
    try {
      if (!this.entityClient.exists(opContext, subscriptionUrn)) {
        throw new RuntimeException(
            String.format("Subscription %s does not exist", subscriptionUrn));
      }

      final AuditStamp auditStamp =
          new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);
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

      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              subscriptionUrn, SUBSCRIPTION_INFO_ASPECT_NAME, subscriptionInfo);
      this.entityClient.ingestProposal(opContext, proposal, false);

      return Map.entry(subscriptionUrn, subscriptionInfo);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update subscription %s", subscriptionUrn), e);
    }
  }

  @Nonnull
  public SearchResult getSubscriptionsSearchResult(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn actorUrn,
      final int start,
      final int count) {
    try {
      if (!this.entityClient.exists(opContext, actorUrn, false)) {
        throw new RuntimeException(String.format("Actor %s does not exist", actorUrn));
      }
      final Filter filter = buildListSubscriptionsFilter(actorUrn);
      return this.entityClient.filter(
          opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, start, count);

    } catch (Exception e) {
      throw new RuntimeException("Failed to search for subscriptions", e);
    }
  }

  @Nonnull
  public Map<Urn, SubscriptionInfo> listSubscriptions(
      @Nonnull OperationContext opContext, @Nonnull final SearchResult searchResult) {
    try {
      final Set<Urn> subscriptionUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      if (subscriptionUrns.isEmpty()) {
        return Collections.emptyMap();
      }

      final Map<Urn, EntityResponse> entityResponseMap =
          this.entityClient.batchGetV2(
              opContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrns, SUBSCRIPTION_ASPECTS);

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(
              entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.entrySet().stream()
          .collect(
              Collectors.toMap(Map.Entry::getKey, entry -> new SubscriptionInfo(entry.getValue())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to list subscriptions", e);
    }
  }

  @Nonnull
  public Map<Urn, SubscriptionInfo> listEntityAssertionSubscriptions(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Integer numMaxSubscriptions) {
    try {
      if (!this.entityClient.exists(systemOpContext, entityUrn, false)) {
        log.warn("Entity {} does not exist", entityUrn);
        return Collections.emptyMap();
      }

      final Filter filter = buildGetSubscriptionsForAssertionFilter(entityUrn, assertionUrn);

      final SearchResult searchResult =
          this.entityClient.filter(
              systemOpContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numMaxSubscriptions);
      final Set<Urn> subscriptionUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entityResponseMap =
          this.entityClient.batchGetV2(
              systemOpContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrns, SUBSCRIPTION_ASPECTS);

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(
              entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new SubscriptionInfo(e.getValue())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to list subscriptions", e);
    }
  }

  /**
   * Note, this is more expensive, used for assertion key deletes where we don't know the entityUrn
   */
  @Nonnull
  public Map<Urn, SubscriptionInfo> listAssertionSubscriptionsWithoutEntityUrn(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Integer numMaxSubscriptions) {
    try {
      final Filter filter = buildGetSubscriptionsForAssertionWithoutEntityUrnFilter(assertionUrn);

      final SearchResult searchResult =
          this.entityClient.filter(
              systemOpContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numMaxSubscriptions);
      final Set<Urn> subscriptionUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entityResponseMap =
          this.entityClient.batchGetV2(
              systemOpContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrns, SUBSCRIPTION_ASPECTS);

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(
              entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new SubscriptionInfo(e.getValue())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to list subscriptions", e);
    }
  }

  public int getNumUserSubscriptionsForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Integer numMaxSubscriptions) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter =
          buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_USER_ENTITY_NAME);
      final SearchResult searchResult =
          this.entityClient.filter(
              opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numMaxSubscriptions);

      return searchResult.hasEntities() ? searchResult.getEntities().size() : 0;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get number of subscriptions for entity %s", entityUrn), e);
    }
  }

  public int getNumGroupSubscriptionsForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Integer numMaxSubscriptions) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter =
          buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_GROUP_ENTITY_NAME);
      final SearchResult searchResult =
          this.entityClient.filter(
              opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numMaxSubscriptions);

      return searchResult.hasEntities() ? searchResult.getEntities().size() : 0;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get number of subscriptions for entity %s", entityUrn), e);
    }
  }

  public List<Urn> getSubscribedUsersForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Integer numSubscribedUsers) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter =
          buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_USER_ENTITY_NAME);
      final SearchResult searchResult =
          this.entityClient.filter(
              opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numSubscribedUsers);

      final Set<Urn> subscriptionUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entityResponseMap =
          this.entityClient.batchGetV2(
              opContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrns, SUBSCRIPTION_ASPECTS);

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(
              entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.values().stream()
          .map(SubscriptionInfo::new)
          .map(SubscriptionInfo::getActorUrn)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get top subscriptions for entity %s", entityUrn), e);
    }
  }

  @Nonnull
  public List<Urn> getGroupSubscribersForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Integer numExampleGroups) {
    try {
      if (!this.entityClient.exists(opContext, entityUrn, false)) {
        throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
      }

      final Filter filter =
          buildGetActorSubscriptionsForEntityFilter(entityUrn, CORP_GROUP_ENTITY_NAME);
      final SearchResult searchResult =
          this.entityClient.filter(
              opContext, SUBSCRIPTION_ENTITY_NAME, filter, null, 0, numExampleGroups);

      final Set<Urn> subscriptionUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entityResponseMap =
          this.entityClient.batchGetV2(
              opContext, SUBSCRIPTION_ENTITY_NAME, subscriptionUrns, SUBSCRIPTION_ASPECTS);

      final Map<Urn, DataMap> subscriptionInfoMap =
          AspectUtils.getDataMapsFromEntityResponseMap(
              entityResponseMap, SUBSCRIPTION_INFO_ASPECT_NAME);

      return subscriptionInfoMap.values().stream()
          .map(SubscriptionInfo::new)
          .map(SubscriptionInfo::getActorUrn)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get top subscriptions for entity %s", entityUrn), e);
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
  Filter buildIsAnyGroupSubscribedFilter(
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
  private Filter buildListSubscriptionsFilter(@Nonnull final Urn actorUrn) {
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
  private Filter buildGetSubscriptionsForAssertionFilter(
      @Nonnull Urn entityUrn, @Nonnull Urn assertionUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    andCriterion.add(buildEntityCriterion(entityUrn));
    andCriterion.add(buildAssertionCriterion(assertionUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private Filter buildGetSubscriptionsForAssertionWithoutEntityUrnFilter(
      @Nonnull Urn assertionUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    andCriterion.add(buildAssertionCriterion(assertionUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private Filter buildGetActorSubscriptionsForEntityFilter(
      @Nonnull final Urn entityUrn, @Nonnull final String actorType) {
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
  private Criterion buildEntityCriterion(@Nonnull final Urn entityUrn) {
    return CriterionUtils.buildCriterion(
        ENTITY_URN_FIELD_NAME, Condition.EQUAL, entityUrn.toString());
  }

  @Nonnull
  private Criterion buildAssertionCriterion(@Nonnull final Urn assertionUrn) {
    return CriterionUtils.buildCriterion(
        ENTITY_CHANGE_TYPES_FILTER_INCLUDE_ASSERTIONS_FIELD_NAME,
        Condition.EQUAL,
        assertionUrn.toString());
  }

  @Nonnull
  private Criterion buildActorCriterion(@Nonnull final Urn actorUrn) {
    return CriterionUtils.buildCriterion(
        ACTOR_URN_FIELD_NAME, Condition.EQUAL, actorUrn.toString());
  }

  @Nonnull
  private Criterion buildActorTypeCriterion(@Nonnull final String actorType) {
    return CriterionUtils.buildCriterion(ACTOR_TYPE_FIELD_NAME, Condition.EQUAL, actorType);
  }
}
