package com.datahub.notification.recipient;

import com.datahub.notification.NotificationScenarioType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.TeamsNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class TeamsNotificationRecipientBuilder extends NotificationRecipientBuilder {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Predicate<? super NotificationSettings> PREDICATE =
      NotificationSettings::hasTeamsSettings;

  public TeamsNotificationRecipientBuilder(@Nonnull final SettingsService settingsService) {
    super(settingsService, PREDICATE);
  }

  private NotificationRecipient buildChannelRecipientWithParams(@Nonnull String recipientId) {
    return buildRecipient(NotificationRecipientType.TEAMS_CHANNEL, recipientId, null);
  }

  private NotificationRecipient buildDMRecipientWithParams(
      @Nonnull String recipientId, @Nullable Urn actorUrn) {
    return buildRecipient(NotificationRecipientType.TEAMS_DM, recipientId, actorUrn);
  }

  @Override
  public List<NotificationRecipient> buildGlobalRecipients(
      @Nonnull OperationContext opContext, @Nonnull final NotificationScenarioType type) {
    final GlobalSettingsInfo globalSettingsInfo = _settingsService.getGlobalSettings(opContext);

    if (globalSettingsInfo == null
        || !globalSettingsInfo.hasNotifications()
        || !globalSettingsInfo.getNotifications().hasSettings()) {
      return Collections.emptyList();
    }

    final NotificationSetting setting =
        globalSettingsInfo.getNotifications().getSettings().get(type.toString());

    // If notifications are disabled for this notification type, skip.
    if (!isTeamsEnabled(globalSettingsInfo) || !isTeamsNotificationEnabled(setting)) {
      // Skip notification type.
      return Collections.emptyList();
    }
    // Teams is enabled. Determine which channel to send to.
    String maybeTeamsChannel =
        hasParam(setting.getParams(), "microsoft-teams.channel")
            ? setting.getParams().get("microsoft-teams.channel")
            : getDefaultTeamsChannel(globalSettingsInfo);

    if (maybeTeamsChannel != null) {
      return ImmutableList.of(buildChannelRecipientWithParams(maybeTeamsChannel));
    } else {
      // No Resolved Teams channel -- warn!
      log.warn(
          String.format(
              "Failed to resolve Teams channel to send notification of type %s to!", type));
      return Collections.emptyList();
    }
  }

  @Override
  public List<NotificationRecipient> buildActorRecipients(
      @NotNull OperationContext opContext,
      @NotNull List<Urn> actorUrns,
      @NotNull NotificationScenarioType type) {
    final List<Urn> userUrns =
        actorUrns.stream()
            .filter(urn -> Constants.CORP_USER_ENTITY_NAME.equals(urn.getEntityType()))
            .collect(Collectors.toList());
    final List<Urn> groupUrns =
        actorUrns.stream()
            .filter(urn -> Constants.CORP_GROUP_ENTITY_NAME.equals(urn.getEntityType()))
            .collect(Collectors.toList());

    final List<NotificationRecipient> recipients = new ArrayList<>();

    if (!userUrns.isEmpty()) {
      recipients.addAll(buildUserActorRecipients(opContext, userUrns, type));
    }

    if (!groupUrns.isEmpty()) {
      recipients.addAll(buildGroupActorRecipients(opContext, groupUrns, type));
    }

    return recipients;
  }

  private List<NotificationRecipient> buildUserActorRecipients(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Urn> userUrns,
      @Nonnull final NotificationScenarioType type) {
    final List<NotificationRecipient> recipients = new ArrayList<>();

    // 1. For each user, extract their settings.
    final Map<Urn, CorpUserSettings> userToSettings =
        _settingsService.batchGetCorpUserSettings(opContext, userUrns);

    // 1.a. Filter out users who have no notification settings.
    final Map<Urn, NotificationSettings> userToNotificationSettings =
        userToSettings.entrySet().stream()
            .filter(entry -> entry.getValue().hasNotificationSettings())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> entry.getValue().getNotificationSettings()));

    // 2. For each user with settings, determine whether we are allowed to send to them based on
    // settings.
    for (final Urn userUrn : userUrns) {

      final NotificationSettings notificationSettings = userToNotificationSettings.get(userUrn);

      if (notificationSettings != null
          && isActorSettingsEnabledForScenario(notificationSettings, type)
          && isTeamsEnabledForActor(userToNotificationSettings, userUrn)) {
        // Determine which Teams handle(s) to send to.
        String maybeTeams =
            extractUserTeamsFromNotificationSettingsForScenarioType(notificationSettings, type);
        if (maybeTeams == null) {
          log.warn(
              "Failed to resolve Teams handle for user {}! Skipping sending notification.",
              userUrn);
          continue;
        }
        // Now we can build and add the recipient.
        recipients.add(buildRecipient(NotificationRecipientType.TEAMS_DM, maybeTeams, userUrn));
      }
    }
    return recipients;
  }

  private List<NotificationRecipient> buildGroupActorRecipients(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Urn> groupUrns,
      @Nonnull final NotificationScenarioType type) {
    final List<NotificationRecipient> recipients = new ArrayList<>();

    // 1. For each group, extract their settings.
    final Map<Urn, CorpGroupSettings> groupToSettings =
        _settingsService.batchGetCorpGroupSettings(opContext, groupUrns);

    // 1.a. Filter out groups who have no notification settings.
    final Map<Urn, NotificationSettings> groupToNotificationSettings =
        groupToSettings.entrySet().stream()
            .filter(entry -> entry.getValue().hasNotificationSettings())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> entry.getValue().getNotificationSettings()));

    // 2. For each group with settings, determine whether we are allowed to send to them based on
    // settings.
    for (final Urn groupUrn : groupUrns) {

      final NotificationSettings notificationSettings = groupToNotificationSettings.get(groupUrn);

      if (notificationSettings != null
          && isActorSettingsEnabledForScenario(notificationSettings, type)
          && isTeamsEnabledForActor(groupToNotificationSettings, groupUrn)) {
        // Determine which Teams channels to send to.
        List<String> maybeTeamsChannels =
            extractGroupTeamsFromNotificationSettingsForScenarioType(notificationSettings, type);
        if (maybeTeamsChannels == null) {
          log.warn(
              "Failed to resolve Teams channels for group {}! Skipping sending notification.",
              groupUrn);
          continue;
        }
        // Now we can build and add the recipients
        maybeTeamsChannels.forEach(
            maybeTeams ->
                recipients.add(
                    buildRecipient(NotificationRecipientType.TEAMS_CHANNEL, maybeTeams, groupUrn)));
      }
    }
    return recipients;
  }

  private boolean isActorSettingsEnabledForScenario(
      @Nonnull final NotificationSettings notificationSettings,
      @Nonnull final NotificationScenarioType type) {
    if (notificationSettings.hasSettings()) {
      final Map<String, NotificationSetting> scenarioSettings = notificationSettings.getSettings();
      if (scenarioSettings.containsKey(type.toString())) {
        return isTeamsNotificationEnabled(scenarioSettings.get(type.toString()));
      }
    }
    return false;
  }

  @Nullable
  private String extractUserTeamsFromNotificationSettingsForScenarioType(
      @Nonnull final NotificationSettings settings, @Nonnull final NotificationScenarioType type) {
    // First, see if there is a scenario-specific override Teams channel
    final Map<String, NotificationSetting> scenarioSettings = settings.getSettings();
    if (scenarioSettings.containsKey(type.toString())) {
      final NotificationSetting setting = scenarioSettings.get(type.toString());
      if (hasParam(setting.getParams(), "microsoft-teams.channel")
          && !setting.getParams().get("microsoft-teams.channel").isEmpty()) {
        return setting.getParams().get("microsoft-teams.channel");
      }
    }
    // First check if there is a default Teams user
    if (settings.hasTeamsSettings() && settings.getTeamsSettings().hasUser()) {
      com.linkedin.settings.global.TeamsUser user = settings.getTeamsSettings().getUser();
      // Create JSON object with all available identifiers
      return createTeamsRecipientId(user);
    }
    // Else, we were unable to resolve a Teams handle
    return null;
  }

  @Nullable
  private List<String> extractGroupTeamsFromNotificationSettingsForScenarioType(
      @Nonnull final NotificationSettings settings, @Nonnull final NotificationScenarioType type) {
    // First, see if there is a scenario-specific override Teams channel
    final Map<String, NotificationSetting> scenarioSettings = settings.getSettings();
    if (scenarioSettings.containsKey(type.toString())) {
      final NotificationSetting setting = scenarioSettings.get(type.toString());
      if (hasParam(setting.getParams(), "microsoft-teams.channel")
          && !setting.getParams().get("microsoft-teams.channel").isEmpty()) {
        return Collections.singletonList(setting.getParams().get("microsoft-teams.channel"));
      }
    }
    // First check if there is a default Teams channels
    if (settings.hasTeamsSettings() && settings.getTeamsSettings().hasChannels()) {
      return settings.getTeamsSettings().getChannels().stream()
          .map(channel -> channel.getId())
          .collect(java.util.stream.Collectors.toList());
    }
    // Else, we were unable to resolve Teams channels
    return null;
  }

  private boolean isTeamsEnabledForActor(
      @Nonnull final Map<Urn, NotificationSettings> actorToNotificationSettings,
      @Nonnull final Urn urn) {
    return actorToNotificationSettings.containsKey(urn)
        && actorToNotificationSettings.get(urn).getSinkTypes().contains(NotificationSinkType.TEAMS);
  }

  @Nullable
  private String getUserRecipientIdFromSubscription(
      Map.Entry<Urn, SubscriptionInfo> urnToSubscriptionInfo) {
    if (urnToSubscriptionInfo.getValue().hasNotificationConfig()
        && urnToSubscriptionInfo.getValue().getNotificationConfig().hasNotificationSettings()
        && urnToSubscriptionInfo
            .getValue()
            .getNotificationConfig()
            .getNotificationSettings()
            .hasTeamsSettings()) {
      TeamsNotificationSettings teamsSettings =
          urnToSubscriptionInfo
              .getValue()
              .getNotificationConfig()
              .getNotificationSettings()
              .getTeamsSettings();
      if (teamsSettings.hasUser()) {
        com.linkedin.settings.global.TeamsUser user = teamsSettings.getUser();
        // Create JSON object with all available identifiers
        return createTeamsRecipientId(user);
      }
      return null;
    }
    return null;
  }

  /*
   * For each user that has a Subscription, try to create a NotificationRecipient object and return this list of NotificationRecipients.
   * If a user has a member ID set on the subscription, use that, otherwise default to what's in their settings
   */
  @Override
  protected List<NotificationRecipient> buildUserSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> userToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> userToNotificationSettings) {
    List<NotificationRecipient> notificationRecipients = new ArrayList<>();
    userToSubscriptionMap
        .entrySet()
        .forEach(
            entry -> {
              // Check if the subscription has Teams settings - if so, create recipients regardless
              // of user settings
              String recipientIdFromSubscription = getUserRecipientIdFromSubscription(entry);
              List<String> channelIdsFromSubscription = getGroupRecipientIdsFromSubscription(entry);

              if (recipientIdFromSubscription != null) {
                // Subscription has a user handle - create DM recipient
                NotificationRecipient notificationRecipient =
                    buildDMRecipientWithParams(recipientIdFromSubscription, entry.getKey());
                notificationRecipients.add(notificationRecipient);
              } else if (channelIdsFromSubscription != null
                  && !channelIdsFromSubscription.isEmpty()) {
                // Subscription has channel IDs - create channel recipients
                for (String channelId : channelIdsFromSubscription) {
                  NotificationRecipient notificationRecipient =
                      buildChannelRecipientWithParams(channelId);
                  notificationRecipients.add(notificationRecipient);
                }
              } else if (isTeamsEnabledForActor(userToNotificationSettings, entry.getKey())) {
                // Fall back to user's personal settings if no subscription-specific settings
                NotificationSettings notificationSettings =
                    userToNotificationSettings.get(entry.getKey());
                if (!notificationSettings.hasTeamsSettings()) {
                  log.warn(
                      String.format(
                          "Unable to create NotificationRecipient for user %s as they do not have Teams Setting configured",
                          entry.getKey()));
                  return;
                }
                String userHandle = null;
                if (notificationSettings.getTeamsSettings().hasUser()) {
                  com.linkedin.settings.global.TeamsUser user =
                      notificationSettings.getTeamsSettings().getUser();
                  // Create JSON object with all available identifiers
                  userHandle = createTeamsRecipientId(user);
                }
                if (userHandle != null) {
                  // User has configured Teams user - create DM recipient
                  String recipientId = userHandle;
                  NotificationRecipient notificationRecipient =
                      buildDMRecipientWithParams(recipientId, entry.getKey());
                  notificationRecipients.add(notificationRecipient);
                } else if (notificationSettings.getTeamsSettings().hasChannels()) {
                  // User has no userHandle but has channels configured - create channel recipients
                  List<String> channelIds =
                      notificationSettings.getTeamsSettings().getChannels().stream()
                          .map(channel -> channel.getId())
                          .collect(java.util.stream.Collectors.toList());
                  for (String channelId : channelIds) {
                    NotificationRecipient notificationRecipient =
                        buildChannelRecipientWithParams(channelId);
                    notificationRecipients.add(notificationRecipient);
                  }
                } else {
                  log.warn(
                      String.format(
                          "Unable to create NotificationRecipient for user %s as they do not have Teams userHandle or channels configured",
                          entry.getKey()));
                  return;
                }
              }
            });
    return notificationRecipients;
  }

  @Nullable
  private List<String> getGroupRecipientIdsFromSubscription(
      Map.Entry<Urn, SubscriptionInfo> urnToSubscriptionInfo) {
    if (urnToSubscriptionInfo.getValue().hasNotificationConfig()
        && urnToSubscriptionInfo.getValue().getNotificationConfig().hasNotificationSettings()
        && urnToSubscriptionInfo
            .getValue()
            .getNotificationConfig()
            .getNotificationSettings()
            .hasTeamsSettings()) {
      TeamsNotificationSettings teamsSettings =
          urnToSubscriptionInfo
              .getValue()
              .getNotificationConfig()
              .getNotificationSettings()
              .getTeamsSettings();
      return teamsSettings.getChannels() != null
          ? teamsSettings.getChannels().stream()
              .map(channel -> channel.getId())
              .collect(java.util.stream.Collectors.toList())
          : null;
    }
    return null;
  }

  /*
   * For each group that has a Subscription, try to create a NotificationRecipient object and return this list of NotificationRecipients.
   * If a group has channels set on the subscription, use that, otherwise default to what's in their settings
   */
  @Override
  protected List<NotificationRecipient> buildGroupSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> groupToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> groupToNotificationSettings) {
    List<NotificationRecipient> notificationRecipients = new ArrayList<>();
    groupToSubscriptionMap
        .entrySet()
        .forEach(
            entry -> {
              // first, ensure Teams is enabled for the group
              if (isTeamsEnabledForActor(groupToNotificationSettings, entry.getKey())) {
                List<String> recipientIdsFromSubscription =
                    getGroupRecipientIdsFromSubscription(entry);
                if (recipientIdsFromSubscription != null
                    && recipientIdsFromSubscription.size() > 0) {
                  recipientIdsFromSubscription.forEach(
                      id -> {
                        notificationRecipients.add(buildChannelRecipientWithParams(id));
                      });
                } else {
                  NotificationSettings notificationSettings =
                      groupToNotificationSettings.get(entry.getKey());
                  if (!notificationSettings.hasTeamsSettings()
                      || !notificationSettings.getTeamsSettings().hasChannels()) {
                    log.warn(
                        String.format(
                            "Unable to create NotificationRecipient for group %s as they do not have Teams Setting configured",
                            entry.getKey()));
                    return;
                  }
                  notificationSettings
                      .getTeamsSettings()
                      .getChannels()
                      .forEach(
                          channel -> {
                            notificationRecipients.add(
                                buildChannelRecipientWithParams(channel.getId()));
                          });
                }
              }
            });
    return notificationRecipients;
  }

  private boolean isTeamsEnabled(@Nullable final GlobalSettingsInfo globalSettingsInfo) {
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().hasTeamsSettings()
        && globalSettingsInfo.getIntegrations().getTeamsSettings().isEnabled();
  }

  private boolean isTeamsNotificationEnabled(@Nonnull final NotificationSetting setting) {
    return hasParam(setting.getParams(), "microsoft-teams.enabled")
        && Boolean.parseBoolean(setting.getParams().get("microsoft-teams.enabled"));
  }

  private String getDefaultTeamsChannel(@Nullable final GlobalSettingsInfo globalSettingsInfo) {
    if (globalSettingsInfo != null && globalSettingsInfo.getIntegrations().hasTeamsSettings()) {
      // First try the new defaultChannel structure
      if (globalSettingsInfo.getIntegrations().getTeamsSettings().hasDefaultChannel()) {
        return globalSettingsInfo.getIntegrations().getTeamsSettings().getDefaultChannel().getId();
      }
      // Fall back to deprecated defaultChannelName
      return globalSettingsInfo.getIntegrations().getTeamsSettings().getDefaultChannelName();
    }
    return null;
  }

  private boolean hasParam(@Nullable final Map<String, String> params, final String param) {
    return params != null && params.containsKey(param);
  }

  /**
   * Creates a JSON-formatted recipient ID containing all available identifiers for a Teams user.
   * This allows the Teams notification sink to try multiple fallback options when sending
   * notifications.
   *
   * @param user The Teams user with potentially multiple identifiers
   * @return JSON string containing available identifiers, or null if no identifiers found
   */
  @Nullable
  private String createTeamsRecipientId(
      @Nonnull final com.linkedin.settings.global.TeamsUser user) {
    Map<String, String> identifiers = new HashMap<>();

    if (user.hasTeamsUserId()) {
      identifiers.put("teams", user.getTeamsUserId());
    }
    if (user.hasAzureUserId()) {
      identifiers.put("azure", user.getAzureUserId());
    }
    if (user.hasEmail()) {
      identifiers.put("email", user.getEmail());
    }

    if (identifiers.isEmpty()) {
      return null;
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(identifiers);
    } catch (Exception e) {
      log.warn(
          "Failed to serialize Teams recipient identifiers to JSON, falling back to single identifier",
          e);
      // Fallback to single identifier with priority order
      if (user.hasTeamsUserId()) {
        return user.getTeamsUserId();
      } else if (user.hasAzureUserId()) {
        return user.getAzureUserId();
      } else if (user.hasEmail()) {
        return user.getEmail();
      }
      return null;
    }
  }
}
