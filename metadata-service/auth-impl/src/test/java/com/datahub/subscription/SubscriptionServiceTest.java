package com.datahub.subscription;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.AcrylConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class SubscriptionServiceTest {
  private static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  private static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  private static final String ENTITY_URN_1_STRING = "urn:li:dataset:1";
  private static final Urn ENTITY_URN_1 = UrnUtils.getUrn(ENTITY_URN_1_STRING);
  private static final String ENTITY_URN_2_STRING = "urn:li:dataset:2";
  private static final Urn ENTITY_URN_2 = UrnUtils.getUrn(ENTITY_URN_2_STRING);
  private static final String SUBSCRIPTION_URN_1_STRING = "urn:li:subscription:1";
  private static final Urn SUBSCRIPTION_URN_1 = UrnUtils.getUrn(SUBSCRIPTION_URN_1_STRING);
  private static final NotificationSinkTypeArray NOTIFICATION_SINK_TYPES =
      new NotificationSinkTypeArray(NotificationSinkType.SLACK);
  private static final SubscriptionNotificationConfig NOTIFICATION_CONFIG =
      new SubscriptionNotificationConfig().setSinkTypes(NOTIFICATION_SINK_TYPES);

  private static final SubscriptionTypeArray SUBSCRIPTION_TYPES_1 =
      new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE, SubscriptionType.UPSTREAM_ENTITY_CHANGE);
  private static final EntityChangeTypeArray ENTITY_CHANGE_TYPES_1 =
      new EntityChangeTypeArray(EntityChangeType.DEPRECATED, EntityChangeType.ASSERTION_FAILED);
  private static final SubscriptionInfo SUBSCRIPTION_INFO_1 = new SubscriptionInfo().setActorUrn(USER_URN)
      .setActorType(com.linkedin.common.ActorType.USER)
      .setTypes(SUBSCRIPTION_TYPES_1)
      .setEntityUrn(ENTITY_URN_1)
      .setEntityChangeTypes(ENTITY_CHANGE_TYPES_1)
      .setNotificationConfig(NOTIFICATION_CONFIG);
  private static final EntityResponse ENTITY_RESPONSE_1 = AspectUtils.createEntityResponseFromAspects(ImmutableMap.of(
      SUBSCRIPTION_INFO_ASPECT_NAME, SUBSCRIPTION_INFO_1));
  private static final SubscriptionTypeArray SUBSCRIPTION_TYPES_2 =
      new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE);
  private static final EntityChangeTypeArray ENTITY_CHANGE_TYPES_2 =
      new EntityChangeTypeArray(EntityChangeType.GLOSSARY_TERM_CHANGE, EntityChangeType.TAG_CHANGE);
  private static final SubscriptionInfo SUBSCRIPTION_INFO_2 = new SubscriptionInfo().setActorUrn(USER_URN)
      .setActorType(com.linkedin.common.ActorType.USER)
      .setTypes(SUBSCRIPTION_TYPES_2)
      .setEntityUrn(ENTITY_URN_2)
      .setEntityChangeTypes(ENTITY_CHANGE_TYPES_2)
      .setNotificationConfig(NOTIFICATION_CONFIG);
  private static final EntityResponse ENTITY_RESPONSE_2 = AspectUtils.createEntityResponseFromAspects(ImmutableMap.of(
      SUBSCRIPTION_INFO_ASPECT_NAME, SUBSCRIPTION_INFO_2));

  private static final String SUBSCRIPTION_URN_2_STRING = "urn:li:subscription:2";
  private static final Urn SUBSCRIPTION_URN_2 = UrnUtils.getUrn(SUBSCRIPTION_URN_2_STRING);
  private static final Set<Urn> SUBSCRIPTION_URNS = ImmutableSet.of(SUBSCRIPTION_URN_1, SUBSCRIPTION_URN_2);
  private static final Set<String> SUBSCRIPTION_ASPECTS = Collections.singleton(SUBSCRIPTION_INFO_ASPECT_NAME);

  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");

  private EntityClient _entityClient;
  private SubscriptionService _subscriptionService;

  @BeforeMethod
  public void setup() {
    _entityClient = mock(EntityClient.class);
    _subscriptionService = new SubscriptionService(_entityClient);
  }

  @Test
  public void testCreateSubscriptionMissingActor() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _subscriptionService.createSubscription(USER_URN, ENTITY_URN_1, SUBSCRIPTION_TYPES_1,
        ENTITY_CHANGE_TYPES_1, NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateSubscriptionMissingEntity() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.exists(eq(ENTITY_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _subscriptionService.createSubscription(USER_URN, ENTITY_URN_1, SUBSCRIPTION_TYPES_1,
        ENTITY_CHANGE_TYPES_1, NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateSubscriptionInvalidActorType() throws Exception {
    when(_entityClient.exists(eq(ENTITY_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);

    assertThrows(() -> _subscriptionService.createSubscription(ENTITY_URN_1, ENTITY_URN_1, SUBSCRIPTION_TYPES_1,
        ENTITY_CHANGE_TYPES_1, NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateSubscription() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.exists(eq(ENTITY_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.ingestProposal(any(MetadataChangeProposal.class), eq(SYSTEM_AUTHENTICATION), eq(true)))
        .thenReturn(SUBSCRIPTION_URN_1_STRING);

    final Map.Entry<Urn, SubscriptionInfo> subscription =
        _subscriptionService.createSubscription(USER_URN, ENTITY_URN_1, SUBSCRIPTION_TYPES_1, ENTITY_CHANGE_TYPES_1,
            NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION);
    assertEquals(subscription, Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
  }

  @Test
  public void testGetSubscriptionInfoMissingSubscription() throws Exception {
    when(_entityClient.exists(eq(SUBSCRIPTION_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _subscriptionService.getSubscriptionInfo(SUBSCRIPTION_URN_1, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetSubscriptionInfoMissingEntityResponse() throws Exception {
    when(_entityClient.exists(eq(SUBSCRIPTION_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(eq(SUBSCRIPTION_ENTITY_NAME), eq(SUBSCRIPTION_URN_1), eq(SUBSCRIPTION_ASPECTS),
        eq(SYSTEM_AUTHENTICATION)))
        .thenReturn(null);

    assertThrows(() -> _subscriptionService.getSubscriptionInfo(SUBSCRIPTION_URN_1, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetSubscriptionInfo() throws Exception {
    when(_entityClient.exists(eq(SUBSCRIPTION_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(eq(SUBSCRIPTION_ENTITY_NAME), eq(SUBSCRIPTION_URN_1), eq(SUBSCRIPTION_ASPECTS),
        eq(SYSTEM_AUTHENTICATION)))
        .thenReturn(ENTITY_RESPONSE_1);

    assertEquals(SUBSCRIPTION_INFO_1,
        _subscriptionService.getSubscriptionInfo(SUBSCRIPTION_URN_1, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testUpdateSubscriptionMissingSubscription() throws Exception {
    when(_entityClient.exists(eq(SUBSCRIPTION_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(
        () -> _subscriptionService.updateSubscription(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1, SUBSCRIPTION_TYPES_1,
            ENTITY_CHANGE_TYPES_1, NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testUpdateSubscription() throws Exception {
    when(_entityClient.exists(eq(SUBSCRIPTION_URN_1), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);

    final Map.Entry<Urn, SubscriptionInfo> subscription =
        _subscriptionService.updateSubscription(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1, SUBSCRIPTION_TYPES_1,
            ENTITY_CHANGE_TYPES_1,
            NOTIFICATION_CONFIG, SYSTEM_AUTHENTICATION);

    verify(_entityClient, times(1)).ingestProposal(any(MetadataChangeProposal.class), eq(SYSTEM_AUTHENTICATION),
        eq(true));
    assertEquals(subscription, Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
  }

  @Test
  public void testListSubscriptionsMissingActor() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _subscriptionService.listSubscriptions(USER_URN, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testListSubscriptionsNoSearchResults() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.filter(
        eq(SUBSCRIPTION_ENTITY_NAME),
        any(),
        any(),
        anyInt(),
        anyInt(),
        eq(SYSTEM_AUTHENTICATION)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listSubscriptions(USER_URN, SYSTEM_AUTHENTICATION);
    assertTrue(subscriptions.isEmpty());
  }

  @Test
  public void testListSubscriptions() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.filter(
        eq(SUBSCRIPTION_ENTITY_NAME),
        any(),
        any(),
        anyInt(),
        anyInt(),
        eq(SYSTEM_AUTHENTICATION)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray(
            new SearchEntity().setEntity(SUBSCRIPTION_URN_1),
            new SearchEntity().setEntity(SUBSCRIPTION_URN_2))));
    when(_entityClient.batchGetV2(
        eq(SUBSCRIPTION_ENTITY_NAME),
        eq(SUBSCRIPTION_URNS),
        eq(SUBSCRIPTION_ASPECTS),
        eq(SYSTEM_AUTHENTICATION)))
        .thenReturn(ImmutableMap.of(
            SUBSCRIPTION_URN_1, ENTITY_RESPONSE_1,
            SUBSCRIPTION_URN_2, ENTITY_RESPONSE_2));

    final Map<Urn, SubscriptionInfo> subscriptions =
        _subscriptionService.listSubscriptions(USER_URN, SYSTEM_AUTHENTICATION);
    final Map<Urn, SubscriptionInfo> expectedSubscriptions = ImmutableMap.of(
        SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1,
        SUBSCRIPTION_URN_2, SUBSCRIPTION_INFO_2);
    assertEquals(subscriptions, expectedSubscriptions);
  }
}
