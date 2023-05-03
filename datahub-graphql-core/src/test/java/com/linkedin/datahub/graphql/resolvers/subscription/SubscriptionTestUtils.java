package com.linkedin.datahub.graphql.resolvers.subscription;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfig;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.List;


public class SubscriptionTestUtils {
  public static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  public static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  public static final String ENTITY_URN_1_STRING = "urn:li:dataset:1";
  public static final Urn ENTITY_URN_1 = UrnUtils.getUrn(ENTITY_URN_1_STRING);
  public static final String ENTITY_URN_2_STRING = "urn:li:dataset:2";
  public static final Urn ENTITY_URN_2 = UrnUtils.getUrn(ENTITY_URN_2_STRING);
  public static final String SUBSCRIPTION_URN_1_STRING = "urn:li:subscription:1";
  public static final Urn SUBSCRIPTION_URN_1 = UrnUtils.getUrn(SUBSCRIPTION_URN_1_STRING);
  public static final List<NotificationSinkType> NOTIFICATION_SINK_GRAPHQL_TYPES =
      ImmutableList.of(NotificationSinkType.SLACK);
  public static final NotificationSinkTypeArray NOTIFICATION_SINK_TYPES =
      new NotificationSinkTypeArray(com.linkedin.event.notification.NotificationSinkType.SLACK);

  public static final String SLACK_USER_HANDLE = "testUser";
  public static final com.linkedin.event.notification.settings.SlackNotificationSettings
      USER_SLACK_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.SlackNotificationSettings().setUserHandle(SLACK_USER_HANDLE);
  public static final com.linkedin.event.notification.settings.NotificationSettings USER_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.NotificationSettings().setSlackSettings(
          USER_SLACK_NOTIFICATION_SETTINGS);
  public static final com.linkedin.subscription.SubscriptionNotificationConfig NOTIFICATION_CONFIG =
      new com.linkedin.subscription.SubscriptionNotificationConfig()
          .setSinkTypes(NOTIFICATION_SINK_TYPES)
          .setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final List<SubscriptionType> SUBSCRIPTION_GRAPHQL_TYPES_1 =
      ImmutableList.of(SubscriptionType.ENTITY_CHANGE, SubscriptionType.UPSTREAM_ENTITY_CHANGE);
  public static final SubscriptionTypeArray SUBSCRIPTION_TYPES_1 =
      new SubscriptionTypeArray(com.linkedin.subscription.SubscriptionType.ENTITY_CHANGE,
          com.linkedin.subscription.SubscriptionType.UPSTREAM_ENTITY_CHANGE);
  public static final List<EntityChangeType> ENTITY_CHANGE_GRAPHQL_TYPES_1 =
      ImmutableList.of(EntityChangeType.DEPRECATED, EntityChangeType.ASSERTION_FAILED);
  public static final EntityChangeTypeArray ENTITY_CHANGE_TYPES_1 =
      new EntityChangeTypeArray(com.linkedin.subscription.EntityChangeType.DEPRECATED,
          com.linkedin.subscription.EntityChangeType.ASSERTION_FAILED);
  public static final SubscriptionInfo SUBSCRIPTION_INFO_1 = new SubscriptionInfo().setActorUrn(USER_URN)
      .setTypes(SUBSCRIPTION_TYPES_1)
      .setEntityUrn(ENTITY_URN_1)
      .setEntityChangeTypes(ENTITY_CHANGE_TYPES_1)
      .setNotificationConfig(NOTIFICATION_CONFIG)
      .setCreatedOn(new AuditStamp().setTime(0L).setActor(USER_URN))
      .setUpdatedOn(new AuditStamp().setTime(0L).setActor(USER_URN));
  public static final String SUBSCRIPTION_URN_2_STRING = "urn:li:subscription:2";
  public static final Urn SUBSCRIPTION_URN_2 = UrnUtils.getUrn(SUBSCRIPTION_URN_2_STRING);
  public static final SubscriptionTypeArray SUBSCRIPTION_TYPES_2 =
      new SubscriptionTypeArray(com.linkedin.subscription.SubscriptionType.ENTITY_CHANGE);
  public static final EntityChangeTypeArray ENTITY_CHANGE_TYPES_2 =
      new EntityChangeTypeArray(com.linkedin.subscription.EntityChangeType.GLOSSARY_TERM_CHANGE,
          com.linkedin.subscription.EntityChangeType.TAG_CHANGE);
  public static final SubscriptionInfo SUBSCRIPTION_INFO_2 = new SubscriptionInfo().setActorUrn(USER_URN)
      .setTypes(SUBSCRIPTION_TYPES_2)
      .setEntityUrn(ENTITY_URN_2)
      .setEntityChangeTypes(ENTITY_CHANGE_TYPES_2)
      .setNotificationConfig(NOTIFICATION_CONFIG)
      .setCreatedOn(new AuditStamp().setTime(0L).setActor(USER_URN))
      .setUpdatedOn(new AuditStamp().setTime(0L).setActor(USER_URN));

  public static SubscriptionNotificationConfig getMappedNotificationConfig() {
    final SubscriptionNotificationConfig notificationConfig = new SubscriptionNotificationConfig();
    notificationConfig.setSinkTypes(NOTIFICATION_SINK_GRAPHQL_TYPES);

    final NotificationSettings notificationSettings = new NotificationSettings();
    final com.linkedin.datahub.graphql.generated.SlackNotificationSettings slackNotificationSettings =
        new com.linkedin.datahub.graphql.generated.SlackNotificationSettings();
    slackNotificationSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    notificationConfig.setNotificationSettings(notificationSettings);

    return notificationConfig;
  }

  public static DataHubSubscription getMappedSubscription1() {
    final DataHubSubscription mappedSubscription1 = new DataHubSubscription();
    mappedSubscription1.setActorUrn(USER_URN_STRING);
    mappedSubscription1.setSubscriptionUrn(SUBSCRIPTION_URN_1_STRING);
    final Dataset dataset = new Dataset();
    dataset.setUrn(ENTITY_URN_1_STRING);
    mappedSubscription1.setEntity(dataset);
    mappedSubscription1.setSubscriptionTypes(
        ImmutableList.of(SubscriptionType.ENTITY_CHANGE, SubscriptionType.UPSTREAM_ENTITY_CHANGE));
    mappedSubscription1.setEntityChangeTypes(
        ImmutableList.of(EntityChangeType.DEPRECATED, EntityChangeType.ASSERTION_FAILED));
    mappedSubscription1.setNotificationConfig(getMappedNotificationConfig());

    return mappedSubscription1;
  }

  public static DataHubSubscription getMappedSubscription2() {
    final DataHubSubscription mappedSubscription2 = new DataHubSubscription();
    mappedSubscription2.setActorUrn(USER_URN_STRING);
    mappedSubscription2.setSubscriptionUrn(SUBSCRIPTION_URN_2_STRING);
    final Dataset dataset = new Dataset();
    dataset.setUrn(ENTITY_URN_2_STRING);
    mappedSubscription2.setEntity(dataset);
    mappedSubscription2.setSubscriptionTypes(ImmutableList.of(SubscriptionType.ENTITY_CHANGE));
    mappedSubscription2.setEntityChangeTypes(
        ImmutableList.of(EntityChangeType.GLOSSARY_TERM_CHANGE, EntityChangeType.TAG_CHANGE));
    mappedSubscription2.setNotificationConfig(getMappedNotificationConfig());

    return mappedSubscription2;
  }

  private SubscriptionTestUtils() {
  }
}
