package com.linkedin.datahub.graphql.resolvers.subscription;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.datahub.graphql.types.subscription.mappers.DataHubSubscriptionMapper;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class SubscriptionResolverUtils {
  @Nonnull
  public static SubscriptionTypeArray mapSubscriptionTypes(@Nonnull List<SubscriptionType> subscriptionTypes) {
    return subscriptionTypes
        .stream()
        .map(subscriptionType -> com.linkedin.subscription.SubscriptionType.valueOf(subscriptionType.toString()))
        .collect(Collectors.toCollection(SubscriptionTypeArray::new));
  }

  @Nonnull
  public static EntityChangeTypeArray mapEntityChangeTypes(@Nonnull List<EntityChangeType> entityChangeTypes) {
    return entityChangeTypes
        .stream()
        .map(entityChangeType -> com.linkedin.subscription.EntityChangeType.valueOf(entityChangeType.toString()))
        .collect(Collectors.toCollection(EntityChangeTypeArray::new));
  }

  @Nonnull
  public static SubscriptionNotificationConfig mapSubscriptionNotificationConfig(
      @Nonnull com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput notificationConfig) {
    return new SubscriptionNotificationConfig()
        .setSinkTypes(notificationConfig.getSinkTypes()
            .stream()
            .map(sinkType -> NotificationSinkType.valueOf(sinkType.toString()))
            .collect(Collectors.toCollection(NotificationSinkTypeArray::new)));
  }
}
