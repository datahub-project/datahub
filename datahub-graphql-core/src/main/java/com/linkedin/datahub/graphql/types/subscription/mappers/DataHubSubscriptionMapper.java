package com.linkedin.datahub.graphql.types.subscription.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfig;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DataHubSubscriptionMapper implements ModelMapper<Map.Entry<Urn, SubscriptionInfo>, DataHubSubscription> {
  private static final String UNKNOWN_ENTITY_TYPE = "UNKNOWN";
  public static final DataHubSubscriptionMapper INSTANCE = new DataHubSubscriptionMapper();

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

    final List<SubscriptionType> subscriptionTypes = subscriptionInfo.getTypes()
        .stream()
        .map(type -> SubscriptionType.valueOf(type.toString()))
        .collect(Collectors.toList());
    result.setSubscriptionTypes(subscriptionTypes);

    final String entityUrnString = subscriptionInfo.hasEntityUrn() ?
        subscriptionInfo.getEntityUrn().toString() : UNKNOWN_ENTITY_TYPE;
    result.setEntityUrn(entityUrnString);

    final List<EntityChangeType> entityChangeTypes = subscriptionInfo.hasEntityChangeTypes() ?
        subscriptionInfo.getEntityChangeTypes()
            .stream()
            .map(type -> EntityChangeType.valueOf(type.toString()))
            .collect(Collectors.toList()) : Collections.emptyList();
    result.setEntityChangeTypes(entityChangeTypes);

    final SubscriptionNotificationConfig notificationConfig = new SubscriptionNotificationConfig();
    final List<NotificationSinkType> notificationSinkTypes = subscriptionInfo.hasNotificationConfig() ?
        subscriptionInfo.getNotificationConfig().getSinkTypes()
            .stream()
            .map(type -> NotificationSinkType.valueOf(type.toString()))
            .collect(Collectors.toList()) : Collections.emptyList();
    notificationConfig.setSinkTypes(notificationSinkTypes);
    result.setNotificationConfig(notificationConfig);

    return result;
  }
}
