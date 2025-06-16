package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionServiceTest {
  private static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  private static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  private static final String ENTITY_URN_1_STRING = "urn:li:dataset:1";
  private static final Urn ENTITY_URN_1 = UrnUtils.getUrn(ENTITY_URN_1_STRING);
  private static final String ENTITY_URN_2_STRING = "urn:li:dataset:2";
  private static final Urn ENTITY_URN_2 = UrnUtils.getUrn(ENTITY_URN_2_STRING);
  private static final String GROUP_URN_1_STRING = "urn:li:corpGroup:1";
  private static final Urn GROUP_URN_1 = UrnUtils.getUrn(GROUP_URN_1_STRING);
  private static final String GROUP_URN_2_STRING = "urn:li:corpGroup:2";
  private static final Urn GROUP_URN_2 = UrnUtils.getUrn(GROUP_URN_2_STRING);

  private static final String SUBSCRIPTION_URN_1_STRING = "urn:li:subscription:1";
  private static final Urn SUBSCRIPTION_URN_1 = UrnUtils.getUrn(SUBSCRIPTION_URN_1_STRING);
  private static final String ASSERTION_URN_1_STRING = "urn:li:assertion:1";
  private static final Urn ASSERTION_URN_1 = UrnUtils.getUrn(ASSERTION_URN_1_STRING);
  private static final NotificationSinkTypeArray NOTIFICATION_SINK_TYPES =
      new NotificationSinkTypeArray(NotificationSinkType.SLACK);
  private static final NotificationSettings NOTIFICATION_SETTINGS =
      new NotificationSettings().setSinkTypes(NOTIFICATION_SINK_TYPES);
  private static final SubscriptionNotificationConfig NOTIFICATION_CONFIG =
      new SubscriptionNotificationConfig().setNotificationSettings(NOTIFICATION_SETTINGS);

  private static final SubscriptionTypeArray SUBSCRIPTION_TYPES_1 =
      new SubscriptionTypeArray(
          SubscriptionType.ENTITY_CHANGE, SubscriptionType.UPSTREAM_ENTITY_CHANGE);
  public static final EntityChangeDetailsArray ENTITY_CHANGE_TYPES_1 =
      new EntityChangeDetailsArray(
          new EntityChangeDetails()
              .setEntityChangeType(com.linkedin.subscription.EntityChangeType.DEPRECATED),
          new EntityChangeDetails()
              .setEntityChangeType(com.linkedin.subscription.EntityChangeType.ASSERTION_FAILED));
  private static final SubscriptionInfo SUBSCRIPTION_INFO_1 =
      new SubscriptionInfo()
          .setActorUrn(USER_URN)
          .setActorType(CORP_USER_ENTITY_NAME)
          .setTypes(SUBSCRIPTION_TYPES_1)
          .setEntityUrn(ENTITY_URN_1)
          .setEntityChangeTypes(ENTITY_CHANGE_TYPES_1)
          .setNotificationConfig(NOTIFICATION_CONFIG);
  private static final EntityResponse ENTITY_RESPONSE_1 =
      AspectUtils.createEntityResponseFromAspects(
          ImmutableMap.of(SUBSCRIPTION_INFO_ASPECT_NAME, SUBSCRIPTION_INFO_1));
  private static final SubscriptionTypeArray SUBSCRIPTION_TYPES_2 =
      new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE);
  public static final EntityChangeDetailsArray ENTITY_CHANGE_TYPES_2 =
      new EntityChangeDetailsArray(
          new EntityChangeDetails()
              .setEntityChangeType(com.linkedin.subscription.EntityChangeType.GLOSSARY_TERM_ADDED),
          new EntityChangeDetails()
              .setEntityChangeType(com.linkedin.subscription.EntityChangeType.TAG_ADDED));
  private static final SubscriptionInfo SUBSCRIPTION_INFO_2 =
      new SubscriptionInfo()
          .setActorUrn(USER_URN)
          .setActorType(CORP_USER_ENTITY_NAME)
          .setTypes(SUBSCRIPTION_TYPES_2)
          .setEntityUrn(ENTITY_URN_2)
          .setEntityChangeTypes(ENTITY_CHANGE_TYPES_2)
          .setNotificationConfig(NOTIFICATION_CONFIG);
  private static final EntityResponse ENTITY_RESPONSE_2 =
      AspectUtils.createEntityResponseFromAspects(
          ImmutableMap.of(SUBSCRIPTION_INFO_ASPECT_NAME, SUBSCRIPTION_INFO_2));

  private static final String SUBSCRIPTION_URN_2_STRING = "urn:li:subscription:2";
  private static final Urn SUBSCRIPTION_URN_2 = UrnUtils.getUrn(SUBSCRIPTION_URN_2_STRING);
  private static final Set<Urn> SUBSCRIPTION_URNS =
      ImmutableSet.of(SUBSCRIPTION_URN_1, SUBSCRIPTION_URN_2);
  private static final Set<String> SUBSCRIPTION_ASPECTS =
      Collections.singleton(SUBSCRIPTION_INFO_ASPECT_NAME);

  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private SystemEntityClient _entityClient;
  private SubscriptionService _subscriptionService;

  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    _entityClient = mock(SystemEntityClient.class);
    this.opContext =
        TestOperationContexts.userContextNoSearchAuthorization(
            Authorizer.EMPTY, SYSTEM_AUTHENTICATION);
    _subscriptionService =
        new SubscriptionService(_entityClient, mock(OpenApiClient.class), objectMapper);
  }

  @Test
  public void testCreateSubscriptionMissingActor() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(false);

    assertThrows(
        () ->
            _subscriptionService.createSubscription(
                opContext,
                USER_URN,
                ENTITY_URN_1,
                SUBSCRIPTION_TYPES_1,
                ENTITY_CHANGE_TYPES_1,
                NOTIFICATION_CONFIG));
  }

  @Test
  public void testCreateSubscriptionMissingEntity() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(true);
    when(_entityClient.exists(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(false);

    assertThrows(
        () ->
            _subscriptionService.createSubscription(
                opContext,
                USER_URN,
                ENTITY_URN_1,
                SUBSCRIPTION_TYPES_1,
                ENTITY_CHANGE_TYPES_1,
                NOTIFICATION_CONFIG));
  }

  @Test
  public void testCreateSubscriptionInvalidActorType() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(true);

    assertThrows(
        () ->
            _subscriptionService.createSubscription(
                opContext,
                ENTITY_URN_1,
                ENTITY_URN_1,
                SUBSCRIPTION_TYPES_1,
                ENTITY_CHANGE_TYPES_1,
                NOTIFICATION_CONFIG));
  }

  @Test
  public void testCreateSubscriptionSubscriptionExists() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(true);
    when(_entityClient.exists(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(true);
    when(_entityClient.filter(
            any(), eq(SUBSCRIPTION_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(new SearchEntity().setEntity(SUBSCRIPTION_URN_1))));

    assertThrows(
        RuntimeException.class,
        () ->
            _subscriptionService.createSubscription(
                opContext,
                USER_URN,
                ENTITY_URN_1,
                SUBSCRIPTION_TYPES_1,
                ENTITY_CHANGE_TYPES_1,
                NOTIFICATION_CONFIG));
  }

  @Test
  public void testCreateSubscription() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(true);
    when(_entityClient.exists(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(true);
    when(_entityClient.filter(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));
    // Don't mock the return value since createSubscription generates its own URN from UUID
    when(_entityClient.ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn("mocked-response");

    final Map.Entry<Urn, SubscriptionInfo> subscription =
        _subscriptionService.createSubscription(
            opContext,
            USER_URN,
            ENTITY_URN_1,
            SUBSCRIPTION_TYPES_1,
            ENTITY_CHANGE_TYPES_1,
            NOTIFICATION_CONFIG);

    // Verify that a subscription URN was created (UUID-based, so we can't predict the exact value)
    assertNotNull(subscription.getKey());
    assertTrue(subscription.getKey().toString().startsWith("urn:li:subscription:"));

    // Verify subscription info contents
    assertEquals(subscription.getValue().getActorType(), SUBSCRIPTION_INFO_1.getActorType());
    assertEquals(subscription.getValue().getActorUrn(), SUBSCRIPTION_INFO_1.getActorUrn());
    assertEquals(
        subscription.getValue().getEntityChangeTypes(), SUBSCRIPTION_INFO_1.getEntityChangeTypes());
    assertEquals(subscription.getValue().getEntityUrn(), SUBSCRIPTION_INFO_1.getEntityUrn());
    assertEquals(
        subscription.getValue().getNotificationConfig(),
        SUBSCRIPTION_INFO_1.getNotificationConfig());
    assertEquals(subscription.getValue().getTypes(), SUBSCRIPTION_INFO_1.getTypes());

    // Verify that ingestProposal was called exactly once (not twice as in previous implementation)
    verify(_entityClient, times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testGetSubscriptionInfoMissingSubscription() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1), any()))
        .thenReturn(false);

    assertThrows(() -> _subscriptionService.getSubscriptionInfo(opContext, SUBSCRIPTION_URN_1));
  }

  @Test
  public void testGetSubscriptionInfoMissingEntityResponse() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1), any()))
        .thenReturn(true);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            eq(SUBSCRIPTION_URN_1),
            eq(SUBSCRIPTION_ASPECTS)))
        .thenReturn(null);

    assertThrows(() -> _subscriptionService.getSubscriptionInfo(opContext, SUBSCRIPTION_URN_1));
  }

  @Test
  public void testGetSubscriptionInfo() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(true);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            eq(SUBSCRIPTION_URN_1),
            eq(SUBSCRIPTION_ASPECTS)))
        .thenReturn(ENTITY_RESPONSE_1);

    assertEquals(
        SUBSCRIPTION_INFO_1,
        _subscriptionService.getSubscriptionInfo(opContext, SUBSCRIPTION_URN_1));
  }

  @Test
  public void testUpdateSubscriptionMissingSubscription() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(false);

    assertThrows(
        () ->
            _subscriptionService.updateSubscription(
                opContext,
                USER_URN,
                SUBSCRIPTION_URN_1,
                SUBSCRIPTION_INFO_1,
                SUBSCRIPTION_TYPES_1,
                ENTITY_CHANGE_TYPES_1,
                NOTIFICATION_CONFIG));
  }

  @Test
  public void testUpdateSubscription() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(true);

    final Map.Entry<Urn, SubscriptionInfo> subscription =
        _subscriptionService.updateSubscription(
            opContext,
            USER_URN,
            SUBSCRIPTION_URN_1,
            SUBSCRIPTION_INFO_1,
            SUBSCRIPTION_TYPES_1,
            ENTITY_CHANGE_TYPES_1,
            NOTIFICATION_CONFIG);

    verify(_entityClient, times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
    assertEquals(subscription, Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
  }

  @Test
  public void testUpdateSubscriptionInfo() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(true);

    final Map.Entry<Urn, SubscriptionInfo> subscription =
        _subscriptionService.updateSubscriptionInfo(
            opContext, SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1);

    verify(_entityClient, times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
    assertEquals(subscription, Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
  }

  @Test
  public void testgetSubscriptionSearchResultMissingActor() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(false);

    assertThrows(
        () -> _subscriptionService.getSubscriptionsSearchResult(opContext, USER_URN, 0, 10));
  }

  @Test
  public void testListSubscriptionsNoSearchResults() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(true);
    when(_entityClient.filter(
            any(), eq(SUBSCRIPTION_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    final SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listSubscriptions(opContext, searchResult);
    assertTrue(subscriptions.isEmpty());
  }

  @Test
  public void testListSubscriptions() throws Exception {
    when(_entityClient.exists(any(OperationContext.class), eq(USER_URN), any())).thenReturn(true);
    when(_entityClient.filter(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_1),
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_2))));
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            eq(SUBSCRIPTION_URNS),
            eq(SUBSCRIPTION_ASPECTS)))
        .thenReturn(
            ImmutableMap.of(
                SUBSCRIPTION_URN_1,
                ENTITY_RESPONSE_1,
                SUBSCRIPTION_URN_2,
                AspectUtils.createEntityResponseFromAspects(
                    ImmutableMap.of(
                        SUBSCRIPTION_INFO_ASPECT_NAME,
                        SUBSCRIPTION_INFO_2.setEntityUrn(ENTITY_URN_1)))));

    final SearchResult searchResult =
        _subscriptionService.getSubscriptionsSearchResult(opContext, USER_URN, 0, 10);
    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listSubscriptions(opContext, searchResult);
    final Map<Urn, SubscriptionInfo> expectedSubscriptions =
        ImmutableMap.of(
            SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1,
            SUBSCRIPTION_URN_2, SUBSCRIPTION_INFO_2);
    assertEquals(subscriptions, expectedSubscriptions);
  }

  @Test
  public void testListEntityAssertionSubscriptions() throws Exception {
    final Integer maxQuery = 1000;
    when(_entityClient.exists(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(true);
    when(_entityClient.filter(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            any(),
            any(),
            anyInt(),
            eq(maxQuery)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_1),
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_2))));
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            eq(SUBSCRIPTION_URNS),
            eq(SUBSCRIPTION_ASPECTS)))
        .thenReturn(
            ImmutableMap.of(
                SUBSCRIPTION_URN_1, ENTITY_RESPONSE_1,
                SUBSCRIPTION_URN_2, ENTITY_RESPONSE_2));

    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listEntityAssertionSubscriptions(
            opContext, ENTITY_URN_1, ASSERTION_URN_1, maxQuery);
    final Map<Urn, SubscriptionInfo> expectedSubscriptions =
        ImmutableMap.of(
            SUBSCRIPTION_URN_1,
            SUBSCRIPTION_INFO_1,
            SUBSCRIPTION_URN_2,
            SUBSCRIPTION_INFO_2.setEntityUrn(ENTITY_URN_1));
    assertEquals(subscriptions, expectedSubscriptions);
  }

  @Test
  public void testListEntityAssertionSubscriptionsWithoutEntityUrn() throws Exception {
    final Integer maxQuery = 1000;
    when(_entityClient.filter(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            any(),
            any(),
            anyInt(),
            eq(maxQuery)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_1),
                        new SearchEntity().setEntity(SUBSCRIPTION_URN_2))));
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(SUBSCRIPTION_ENTITY_NAME),
            eq(SUBSCRIPTION_URNS),
            eq(SUBSCRIPTION_ASPECTS)))
        .thenReturn(
            ImmutableMap.of(
                SUBSCRIPTION_URN_1, ENTITY_RESPONSE_1,
                SUBSCRIPTION_URN_2, ENTITY_RESPONSE_2));

    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listAssertionSubscriptionsWithoutEntityUrn(
            opContext, ASSERTION_URN_1, maxQuery);
    final Map<Urn, SubscriptionInfo> expectedSubscriptions =
        ImmutableMap.of(
            SUBSCRIPTION_URN_1,
            SUBSCRIPTION_INFO_1,
            SUBSCRIPTION_URN_2,
            SUBSCRIPTION_INFO_2.setEntityUrn(ENTITY_URN_1));
    assertEquals(subscriptions, expectedSubscriptions);
  }

  @Test
  public void testIsAnyGroupSubscribedValidGroups() throws Exception {
    when(_entityClient.exists(same(opContext), eq(ENTITY_URN_1), any())).thenReturn(true);

    Filter expectedFilter =
        _subscriptionService.buildIsAnyGroupSubscribedFilter(
            ENTITY_URN_1, ImmutableList.of(GROUP_URN_1, GROUP_URN_2));
    when(_entityClient.filter(
            any(), eq(SUBSCRIPTION_ENTITY_NAME), eq(expectedFilter), any(), anyInt(), anyInt()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    when(_entityClient.exists(same(opContext), eq(GROUP_URN_1), any())).thenReturn(true);
    when(_entityClient.exists(same(opContext), eq(GROUP_URN_2), any())).thenReturn(true);

    assertFalse(
        _subscriptionService.isAnyGroupSubscribed(
            opContext, ENTITY_URN_1, ImmutableList.of(GROUP_URN_1, GROUP_URN_2)));
  }

  @Test
  public void testIsAnyGroupSubscribedInvalidGroup() throws Exception {
    when(_entityClient.exists(same(opContext), eq(ENTITY_URN_1), any())).thenReturn(true);

    Filter expectedFilter =
        _subscriptionService.buildIsAnyGroupSubscribedFilter(
            ENTITY_URN_1, ImmutableList.of(GROUP_URN_1));
    when(_entityClient.filter(
            any(), eq(SUBSCRIPTION_ENTITY_NAME), eq(expectedFilter), any(), anyInt(), anyInt()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    when(_entityClient.exists(same(opContext), eq(GROUP_URN_1), any())).thenReturn(true);
    when(_entityClient.exists(same(opContext), eq(GROUP_URN_2), any())).thenReturn(false);

    assertFalse(
        _subscriptionService.isAnyGroupSubscribed(
            opContext, ENTITY_URN_1, ImmutableList.of(GROUP_URN_1, GROUP_URN_2)));
  }
}
