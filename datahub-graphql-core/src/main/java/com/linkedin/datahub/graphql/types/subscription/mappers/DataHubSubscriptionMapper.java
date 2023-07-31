package com.linkedin.datahub.graphql.types.subscription.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfig;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DataHubSubscriptionMapper implements ModelMapper<Map.Entry<Urn, SubscriptionInfo>, DataHubSubscription> {
  public static final DataHubSubscriptionMapper INSTANCE = new DataHubSubscriptionMapper();
  public static final AuditStampMapper AUDIT_STAMP_MAPPER = new AuditStampMapper();

  public static DataHubSubscription map(final Map.Entry<Urn, SubscriptionInfo> subscription) {
    return INSTANCE.apply(subscription);
  }

  @Override
  public DataHubSubscription apply(final Map.Entry<Urn, SubscriptionInfo> subscription) {
    final Urn subscriptionUrn = subscription.getKey();
    final SubscriptionInfo subscriptionInfo = subscription.getValue();
    final DataHubSubscription result = new DataHubSubscription();
    result.setActorUrn(subscriptionInfo.getActorUrn().toString());
    result.setSubscriptionUrn(subscriptionUrn.toString());
    result.setCreatedOn(AUDIT_STAMP_MAPPER.apply(subscriptionInfo.getCreatedOn()));
    result.setUpdatedOn(AUDIT_STAMP_MAPPER.apply(subscriptionInfo.getUpdatedOn()));

    final List<SubscriptionType> subscriptionTypes = subscriptionInfo.getTypes()
        .stream()
        .map(type -> SubscriptionType.valueOf(type.toString()))
        .collect(Collectors.toList());
    result.setSubscriptionTypes(subscriptionTypes);

    final Entity entity = UrnToEntityMapper.map(subscriptionInfo.getEntityUrn());
    result.setEntity(entity);

    final List<EntityChangeType> entityChangeTypes =
        subscriptionInfo.hasEntityChangeTypes() ? subscriptionInfo.getEntityChangeTypes()
            .stream()
            .map(type -> EntityChangeType.valueOf(type.toString()))
            .collect(Collectors.toList()) : Collections.emptyList();
    result.setEntityChangeTypes(entityChangeTypes);

    if (subscriptionInfo.hasNotificationConfig()) {
      final SubscriptionNotificationConfig notificationConfig =
          mapNotificationConfig(subscriptionInfo.getNotificationConfig());
      result.setNotificationConfig(notificationConfig);
    }

    return result;
  }

  private SubscriptionNotificationConfig mapNotificationConfig(
      @Nonnull final com.linkedin.subscription.SubscriptionNotificationConfig notificationConfig) {
    final SubscriptionNotificationConfig result = new SubscriptionNotificationConfig();

    if (notificationConfig.hasNotificationSettings()) {
      final NotificationSettings notificationSettings =
          NotificationSettingsMapper.map(notificationConfig.getNotificationSettings());
      result.setNotificationSettings(notificationSettings);
    }

    return result;
  }
}
