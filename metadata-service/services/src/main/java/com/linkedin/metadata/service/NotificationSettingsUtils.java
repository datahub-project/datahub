package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import javax.annotation.Nonnull;

/**
 * Utility class for creating default notification settings. This provides a single source of truth
 * for default notification settings used across the platform.
 */
public final class NotificationSettingsUtils {

  // Notification scenario type constants
  private static final String NEW_PROPOSAL = "NEW_PROPOSAL";
  private static final String PROPOSAL_STATUS_CHANGE = "PROPOSAL_STATUS_CHANGE";
  private static final String PROPOSER_PROPOSAL_STATUS_CHANGE = "PROPOSER_PROPOSAL_STATUS_CHANGE";
  private static final String NEW_ACTION_WORKFLOW_FORM_REQUEST = "NEW_ACTION_WORKFLOW_FORM_REQUEST";
  private static final String REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE =
      "REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE";
  private static final String DATA_HUB_COMMUNITY_UPDATES = "DATA_HUB_COMMUNITY_UPDATES";

  private NotificationSettingsUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Creates default email notification scenario settings.
   *
   * @param enableDataHubCommunityUpdates whether to enable DATA_HUB_COMMUNITY_UPDATES notifications
   * @return NotificationSettingMap with default email notification settings
   */
  public static NotificationSettingMap createDefaultEmailNotificationScenarioSettings(
      boolean enableDataHubCommunityUpdates) {
    ImmutableMap.Builder<String, NotificationSetting> builder =
        ImmutableMap.<String, NotificationSetting>builder()
            // Enable being notified when I'm assigned to a proposal
            .put(
                NEW_PROPOSAL,
                new NotificationSetting()
                    .setValue(NotificationSettingValue.ENABLED)
                    .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))))
            // Enable being notified when a proposed I'm assigned to is approved or rejected
            .put(
                PROPOSAL_STATUS_CHANGE,
                new NotificationSetting()
                    .setValue(NotificationSettingValue.ENABLED)
                    .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))))
            // Enable being notified when a proposal I've created is approved or rejected
            .put(
                PROPOSER_PROPOSAL_STATUS_CHANGE,
                new NotificationSetting()
                    .setValue(NotificationSettingValue.ENABLED)
                    .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))))
            // Enable being notified when assigned a new action workflow request.
            .put(
                NEW_ACTION_WORKFLOW_FORM_REQUEST,
                new NotificationSetting()
                    .setValue(NotificationSettingValue.ENABLED)
                    .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))))
            // Enable being notified when an action workflow request step is completed
            .put(
                REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE,
                new NotificationSetting()
                    .setValue(NotificationSettingValue.ENABLED)
                    .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))));

    // Conditionally add DataHub community updates based on parameter
    if (enableDataHubCommunityUpdates) {
      builder.put(
          DATA_HUB_COMMUNITY_UPDATES,
          new NotificationSetting()
              .setValue(NotificationSettingValue.ENABLED)
              .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))));
    } else {
      builder.put(
          DATA_HUB_COMMUNITY_UPDATES,
          new NotificationSetting()
              .setValue(NotificationSettingValue.DISABLED)
              .setParams(new StringMap(ImmutableMap.of("email.enabled", "false"))));
    }

    return new NotificationSettingMap(builder.build());
  }

  /**
   * Creates default notification settings with email configuration.
   *
   * @param email the email address to use for notifications
   * @param enableDataHubCommunityUpdates whether to enable DATA_HUB_COMMUNITY_UPDATES notifications
   * @return NotificationSettings with default email notification settings
   */
  public static NotificationSettings createDefaultNotificationSettingsWithEmail(
      @Nonnull String email, boolean enableDataHubCommunityUpdates) {
    NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.EMAIL)));
    notificationSettings.setEmailSettings(new EmailNotificationSettings().setEmail(email));
    notificationSettings.setSettings(
        createDefaultEmailNotificationScenarioSettings(enableDataHubCommunityUpdates));
    return notificationSettings;
  }
}
