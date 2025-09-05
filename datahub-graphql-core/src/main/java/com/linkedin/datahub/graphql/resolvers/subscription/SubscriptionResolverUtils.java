package com.linkedin.datahub.graphql.resolvers.subscription;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EntityChangeDetailsFilterInput;
import com.linkedin.datahub.graphql.generated.EntityChangeDetailsInput;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.event.notification.settings.TeamsNotificationSettings;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeDetailsFilter;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionTypeArray;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubscriptionResolverUtils {
  @Nonnull
  public static SubscriptionTypeArray mapSubscriptionTypes(
      @Nonnull List<SubscriptionType> subscriptionTypes) {
    final SubscriptionTypeArray result = new SubscriptionTypeArray();
    for (SubscriptionType subscriptionType : subscriptionTypes) {
      try {
        result.add(com.linkedin.subscription.SubscriptionType.valueOf(subscriptionType.toString()));
      } catch (IllegalArgumentException e) {
        log.warn(
            String.format("Unable to map subscription type: %s. Skipping...", subscriptionType));
      }
    }
    return result;
  }

  @Nonnull
  public static EntityChangeDetailsArray mapEntityChangeDetails(
      @Nonnull List<EntityChangeDetailsInput> entityChangeDetails) {
    final EntityChangeDetailsArray result = new EntityChangeDetailsArray();
    for (EntityChangeDetailsInput entityChangeDetail : entityChangeDetails) {
      try {
        EntityChangeDetails changeDetails = new EntityChangeDetails();
        changeDetails.setEntityChangeType(
            EntityChangeType.valueOf(entityChangeDetail.getEntityChangeType().toString()));
        if (entityChangeDetail.getFilter() != null) {
          changeDetails.setFilter(mapEntityChangeDetailsFilter(entityChangeDetail.getFilter()));
        }
        result.add(changeDetails);
      } catch (IllegalArgumentException e) {
        log.warn(
            String.format("Unable to map entity change type: %s. Skipping...", entityChangeDetail));
      }
    }
    return result;
  }

  @Nonnull
  public static EntityChangeDetailsFilter mapEntityChangeDetailsFilter(
      @Nonnull EntityChangeDetailsFilterInput filterInput) {
    EntityChangeDetailsFilter filter = new EntityChangeDetailsFilter();
    if (filterInput.getIncludeAssertions() == null) {
      return filter;
    }
    try {
      UrnArray urnArray =
          new UrnArray(
              filterInput.getIncludeAssertions().stream()
                  .map(UrnUtils::getUrn)
                  .collect(Collectors.toSet()));
      filter.setIncludeAssertions(urnArray);
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unable to map entity change filter: %s. Skipping...",
              filterInput.getIncludeAssertions()));
    }
    return filter;
  }

  @Nonnull
  public static SubscriptionNotificationConfig mapSubscriptionNotificationConfig(
      @Nonnull
          com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput
              notificationConfig) {
    final SubscriptionNotificationConfig result = new SubscriptionNotificationConfig();

    if (notificationConfig.getNotificationSettings() != null) {
      final NotificationSettings notificationSettings =
          mapNotificationSettings(notificationConfig.getNotificationSettings());
      result.setNotificationSettings(notificationSettings);
    }

    return result;
  }

  @Nonnull
  public static NotificationSettings mapNotificationSettings(
      @Nonnull
          com.linkedin.datahub.graphql.generated.NotificationSettingsInput notificationSettings) {
    final NotificationSettings result = new NotificationSettings();
    final NotificationSinkTypeArray sinkTypes = new NotificationSinkTypeArray();

    if (notificationSettings.getSinkTypes() != null) {
      for (com.linkedin.datahub.graphql.generated.NotificationSinkType sinkType :
          notificationSettings.getSinkTypes()) {
        try {
          sinkTypes.add(NotificationSinkType.valueOf(sinkType.toString()));
        } catch (IllegalArgumentException e) {
          log.warn(
              String.format("Unable to map notification sink type: %s. Skipping...", sinkType));
        }
      }
      result.setSinkTypes(sinkTypes);
    }

    if (notificationSettings.getSlackSettings() != null) {
      result.setSlackSettings(
          mapSlackNotificationSettings(notificationSettings.getSlackSettings()));
    }

    if (notificationSettings.getEmailSettings() != null) {
      result.setEmailSettings(
          mapEmailNotificationSettings(notificationSettings.getEmailSettings()));
    }

    if (notificationSettings.getTeamsSettings() != null) {
      result.setTeamsSettings(
          mapTeamsNotificationSettings(notificationSettings.getTeamsSettings()));
    }

    return result;
  }

  @Nonnull
  public static SlackNotificationSettings mapSlackNotificationSettings(
      @Nonnull
          com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput slackSettings) {
    final SlackNotificationSettings result = new SlackNotificationSettings();
    if (slackSettings.getUserHandle() != null) {
      result.setUserHandle(slackSettings.getUserHandle());
    }
    if (slackSettings.getChannels() != null && !slackSettings.getChannels().isEmpty()) {
      result.setChannels(new StringArray(slackSettings.getChannels()));
    }

    return result;
  }

  @Nonnull
  public static EmailNotificationSettings mapEmailNotificationSettings(
      @Nonnull
          com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput emailSettings) {
    final EmailNotificationSettings result = new EmailNotificationSettings();
    if (emailSettings.getEmail() != null) {
      result.setEmail(emailSettings.getEmail());
    }
    return result;
  }

  @Nonnull
  public static TeamsNotificationSettings mapTeamsNotificationSettings(
      @Nonnull
          com.linkedin.datahub.graphql.generated.TeamsNotificationSettingsInput teamsSettings) {
    final TeamsNotificationSettings result = new TeamsNotificationSettings();
    if (teamsSettings.getUser() != null) {
      com.linkedin.settings.global.TeamsUser user = new com.linkedin.settings.global.TeamsUser();
      if (teamsSettings.getUser().getTeamsUserId() != null) {
        user.setTeamsUserId(teamsSettings.getUser().getTeamsUserId());
      }
      if (teamsSettings.getUser().getAzureUserId() != null) {
        user.setAzureUserId(teamsSettings.getUser().getAzureUserId());
      }
      if (teamsSettings.getUser().getEmail() != null) {
        user.setEmail(teamsSettings.getUser().getEmail());
      }
      if (teamsSettings.getUser().getDisplayName() != null) {
        user.setDisplayName(teamsSettings.getUser().getDisplayName());
      }
      result.setUser(user);
    }
    if (teamsSettings.getChannels() != null && !teamsSettings.getChannels().isEmpty()) {
      com.linkedin.settings.global.TeamsChannelArray channelArray =
          new com.linkedin.settings.global.TeamsChannelArray();
      for (com.linkedin.datahub.graphql.generated.TeamsChannelInput channelInput :
          teamsSettings.getChannels()) {
        com.linkedin.settings.global.TeamsChannel channel =
            new com.linkedin.settings.global.TeamsChannel();
        channel.setId(channelInput.getId());
        if (channelInput.getName() != null) {
          channel.setName(channelInput.getName());
        }
        channel.setLastUpdated(System.currentTimeMillis());
        channelArray.add(channel);
      }
      result.setChannels(channelArray);
    }

    return result;
  }

  private SubscriptionResolverUtils() {}
}
