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
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SubscriptionResolverUtils {
  @Nonnull
  public static SubscriptionTypeArray mapSubscriptionTypes(@Nonnull List<SubscriptionType> subscriptionTypes) {
    final SubscriptionTypeArray result = new SubscriptionTypeArray();
    for (SubscriptionType subscriptionType : subscriptionTypes) {
      try {
        result.add(com.linkedin.subscription.SubscriptionType.valueOf(subscriptionType.toString()));
      } catch (IllegalArgumentException e) {
        log.warn(String.format("Unable to map subscription type: %s. Skipping...", subscriptionType));
      }
    }
    return result;
  }

  @Nonnull
  public static EntityChangeTypeArray mapEntityChangeTypes(@Nonnull List<EntityChangeType> entityChangeTypes) {
    final EntityChangeTypeArray result = new EntityChangeTypeArray();
    for (EntityChangeType entityChangeType : entityChangeTypes) {
      try {
        result.add(com.linkedin.subscription.EntityChangeType.valueOf(entityChangeType.toString()));
      } catch (IllegalArgumentException e) {
        log.warn(String.format("Unable to map entity change type: %s. Skipping...", entityChangeType));
      }
    }
    return result;
  }

  @Nonnull
  public static SubscriptionNotificationConfig mapSubscriptionNotificationConfig(
      @Nonnull com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput notificationConfig) {
    final SubscriptionNotificationConfig result = new SubscriptionNotificationConfig();
    final NotificationSinkTypeArray sinkTypes = new NotificationSinkTypeArray();
    for (com.linkedin.datahub.graphql.generated.NotificationSinkType sinkType : notificationConfig.getSinkTypes()) {
      try {
        sinkTypes.add(NotificationSinkType.valueOf(sinkType.toString()));
      } catch (IllegalArgumentException e) {
        log.warn(String.format("Unable to map notification sink type: %s. Skipping...", sinkType));
      }
    }
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
