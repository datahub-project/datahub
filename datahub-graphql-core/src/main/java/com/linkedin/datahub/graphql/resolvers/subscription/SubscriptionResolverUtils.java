package com.linkedin.datahub.graphql.resolvers.subscription;

import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.List;
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
    final SubscriptionNotificationConfig result = new SubscriptionNotificationConfig();
    final NotificationSinkTypeArray sinkTypes = notificationConfig.getSinkTypes()
        .stream()
        .map(sinkType -> NotificationSinkType.valueOf(sinkType.toString()))
        .collect(Collectors.toCollection(NotificationSinkTypeArray::new));
    result.setSinkTypes(sinkTypes);

    if (notificationConfig.getNotificationSettings() != null) {
      final NotificationSettings notificationSettings =
          mapNotificationSettings(notificationConfig.getNotificationSettings());
      result.setNotificationSettings(notificationSettings);
    }

    return result;
  }

  @Nonnull
  public static NotificationSettings mapNotificationSettings(
      @Nonnull com.linkedin.datahub.graphql.generated.NotificationSettingsInput notificationSettings) {
    final NotificationSettings result = new NotificationSettings();
    if (notificationSettings.getSlackSettings() != null) {
      result.setSlackSettings(mapSlackNotificationSettings(notificationSettings.getSlackSettings()));
    }

    return result;
  }

  @Nonnull
  public static SlackNotificationSettings mapSlackNotificationSettings(
      @Nonnull com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput slackSettings) {
    final SlackNotificationSettings result = new SlackNotificationSettings();
    if (slackSettings.getUserHandle() != null) {
      result.setUserHandle(slackSettings.getUserHandle());
    }
    if (slackSettings.getChannels() != null && !slackSettings.getChannels().isEmpty()) {
      result.setChannels(new StringArray(slackSettings.getChannels()));
    }

    return result;
  }

  private SubscriptionResolverUtils() {
  }
}
