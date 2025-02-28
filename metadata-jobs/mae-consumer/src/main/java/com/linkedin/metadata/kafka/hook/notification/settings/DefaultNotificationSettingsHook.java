package com.linkedin.metadata.kafka.hook.notification.settings;

import static com.linkedin.metadata.Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.service.SettingsService.DEFAULT_CORP_USER_SETTINGS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.settings.SettingsServiceFactory;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Assigns a default email address for users and groups if they do not have one set in their
 * personal notification settings. It is useful for growth hacking notifications by seeding an email
 * address.
 *
 * <p>The logic is as follows:
 *
 * <p>- When a user or group is created, the hook will attempt to find an email address (e.g. from
 * signup or SSO flow) and then will create a default instance of the notification settings for the
 * user or group.
 *
 * <p>This hook is enabled by default and can be disabled in application configurations.
 */
@Slf4j
@Component
@Import({SettingsServiceFactory.class, SystemAuthenticationFactory.class})
public class DefaultNotificationSettingsHook implements MetadataChangeLogHook {

  private static final Set<String> SUPPORTED_ENTITY_TYPES =
      ImmutableSet.of(CORP_USER_ENTITY_NAME, CORP_GROUP_ENTITY_NAME);

  private static final Set<String> SUPPORTED_ASPECT_TYPES =
      ImmutableSet.of(
          CORP_USER_INFO_ASPECT_NAME,
          CORP_USER_EDITABLE_INFO_NAME,
          CORP_GROUP_INFO_ASPECT_NAME,
          CORP_GROUP_EDITABLE_INFO_ASPECT_NAME);
  private final SettingsService settingsService;
  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  private final boolean isEnabled;

  @Autowired
  public DefaultNotificationSettingsHook(
      @Nonnull final SettingsService settingsService,
      @Nonnull @Value("${notifications.defaultSettingsHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${notifications.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.settingsService =
        Objects.requireNonNull(settingsService, "settingsService must not be null");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public DefaultNotificationSettingsHook(
      @Nonnull final SettingsService settingsService, @Nonnull Boolean isEnabled) {
    this(settingsService, isEnabled, "");
  }

  @Override
  public DefaultNotificationSettingsHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized Default Notification Settings hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return this.isEnabled;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (this.isEnabled && isEligibleForProcessing(event)) {
      log.debug(
          "Found user or group info change event. Attempting to populate default notification settings.");
      if (event.getEntityType().equals(CORP_USER_ENTITY_NAME)) {
        handleUserUpdate(event);
      } else if (event.getEntityType().equals(CORP_GROUP_ENTITY_NAME)) {
        handleGroupUpdate(event);
      }
    }
  }

  private void handleUserUpdate(@Nonnull MetadataChangeLog event) {
    final Urn userUrn = event.getEntityUrn();
    if (userUrn == null) {
      log.error("Unexpected found user urn to be null. Skipping processing.");
      return;
    }
    final String aspectName = event.getAspectName();
    if (aspectName.equals(CORP_USER_INFO_ASPECT_NAME)) {
      handleUserInfoUpdate(
          systemOperationContext,
          userUrn,
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              CorpUserInfo.class));
    } else if (aspectName.equals(CORP_USER_EDITABLE_INFO_NAME)) {
      handleUserEditableInfoUpdate(
          systemOperationContext,
          userUrn,
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              CorpUserEditableInfo.class));
    }
  }

  private void handleGroupUpdate(@Nonnull MetadataChangeLog event) {
    final Urn groupUrn = event.getEntityUrn();
    if (groupUrn == null) {
      log.error("Unexpected found group urn to be null. Skipping processing.");
      return;
    }
    final String aspectName = event.getAspectName();
    if (aspectName.equals(CORP_GROUP_INFO_ASPECT_NAME)) {
      handleGroupInfoUpdate(
          systemOperationContext,
          groupUrn,
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              CorpGroupInfo.class));
    } else if (aspectName.equals(CORP_GROUP_EDITABLE_INFO_ASPECT_NAME)) {
      handleGroupEditableInfoUpdate(
          systemOperationContext,
          groupUrn,
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              CorpGroupEditableInfo.class));
    }
  }

  private void handleUserInfoUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull CorpUserInfo userInfo) {
    if (userInfo.getEmail() == null) {
      log.debug("User {} has no email address. Skipping updating notification settings", userUrn);
    } else {
      handleUserEmailUpdate(opContext, userUrn, userInfo.getEmail());
    }
  }

  private void handleUserEditableInfoUpdate(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      @Nonnull CorpUserEditableInfo userEditableInfo) {
    if (userEditableInfo.getEmail() == null) {
      log.debug(
          "User {} has no email address. Skipping updating email notification settings", userUrn);
    } else {
      handleUserEmailUpdate(opContext, userUrn, userEditableInfo.getEmail());
    }
    if (userEditableInfo.getSlack() == null) {
      log.debug(
          "User {} has no slack handle. Skipping updating slack notification settings", userUrn);
    } else {
      handleUserSlackUpdate(opContext, userUrn, userEditableInfo.getSlack());
    }
  }

  private void handleUserSlackUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull String handle) {
    final CorpUserSettings userSettings = settingsService.getCorpUserSettings(opContext, userUrn);
    if (userSettings != null
        && userSettings.getNotificationSettings() != null
        && userSettings.getNotificationSettings().getSlackSettings() != null
        && userSettings.getNotificationSettings().getSlackSettings().getUserHandle() != null
        && !userSettings.getNotificationSettings().getSlackSettings().getUserHandle().isEmpty()) {
      log.debug(
          "User {} already has slack notification settings. Skipping default notification settings creation.",
          userUrn);
      return;
    }
    // If there are no notification settings. We can feel free to override.
    final CorpUserSettings corpUserSettings =
        userSettings == null ? DEFAULT_CORP_USER_SETTINGS : userSettings;
    log.debug("Creating default notification settings for user {}", userUrn);
    final NotificationSettings newSettings;
    if (corpUserSettings.getNotificationSettings() != null) {
      newSettings = corpUserSettings.getNotificationSettings();
    } else {
      newSettings = new NotificationSettings().setSinkTypes(new NotificationSinkTypeArray());
    }
    applyDefaultNotificationSettingsWithSlack(newSettings, handle);

    settingsService.updateCorpUserSettings(
        opContext, userUrn, corpUserSettings.setNotificationSettings(newSettings));
  }

  private void handleUserEmailUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull String email) {
    final CorpUserSettings userSettings = settingsService.getCorpUserSettings(opContext, userUrn);
    if (userSettings != null && userSettings.getNotificationSettings() != null) {
      log.debug(
          "User {} already has notification settings. Skipping default notification settings creation.",
          userUrn);
      return;
    }
    // Then there are no notification settings. We can feel free to override.
    final CorpUserSettings newSettings =
        userSettings == null ? DEFAULT_CORP_USER_SETTINGS : userSettings;
    log.debug("Creating default notification settings for user {}", userUrn);
    final NotificationSettings defaultSettings = createDefaultNotificationSettingsWithEmail(email);
    settingsService.updateCorpUserSettings(
        opContext, userUrn, newSettings.setNotificationSettings(defaultSettings));
  }

  private void handleGroupInfoUpdate(
      @Nonnull OperationContext opContext,
      @Nonnull Urn groupUrn,
      @Nonnull CorpGroupInfo groupInfo) {
    if (groupInfo.getEmail() == null) {
      log.debug("Group {} has no email address. Skipping updating notification settings", groupUrn);
      return;
    }
    handleGroupEmailUpdate(opContext, groupUrn, groupInfo.getEmail());
  }

  private void handleGroupEditableInfoUpdate(
      @Nonnull OperationContext opContext,
      @Nonnull Urn groupUrn,
      @Nonnull CorpGroupEditableInfo groupEditableInfo) {
    if (groupEditableInfo.getEmail() == null) {
      log.debug("Group {} has no email address. Skipping updating notification settings", groupUrn);
      return;
    }
    handleGroupEmailUpdate(opContext, groupUrn, groupEditableInfo.getEmail());
  }

  private void handleGroupEmailUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn groupUrn, @Nonnull String email) {
    final CorpGroupSettings groupSettings =
        settingsService.getCorpGroupSettings(opContext, groupUrn);
    if (groupSettings != null && groupSettings.getNotificationSettings() != null) {
      log.debug(
          "Group {} already has notification settings. Skipping default notification settings creation.",
          groupUrn);
      return;
    }
    // Then there are no notification settings. We can feel free to override.
    final CorpGroupSettings newSettings =
        groupSettings == null ? new CorpGroupSettings() : groupSettings;
    log.debug("Creating default notification settings for group {}", groupUrn);
    final NotificationSettings defaultSettings = createDefaultNotificationSettingsWithEmail(email);
    settingsService.updateCorpGroupSettings(
        opContext, groupUrn, newSettings.setNotificationSettings(defaultSettings));
  }

  private NotificationSettings createDefaultNotificationSettingsWithEmail(@Nonnull String email) {
    NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.EMAIL)));
    notificationSettings.setEmailSettings(new EmailNotificationSettings().setEmail(email));
    return notificationSettings;
  }

  private void applyDefaultNotificationSettingsWithSlack(
      @Nonnull NotificationSettings existingSettings, @Nonnull String handle) {
    final List<NotificationSinkType> sinkTypes =
        Stream.concat(
                !existingSettings.hasSinkTypes()
                    ? Stream.empty()
                    : existingSettings.getSinkTypes().stream(),
                Stream.of(NotificationSinkType.SLACK))
            .sorted()
            .collect(Collectors.toList());
    existingSettings.setSinkTypes(new NotificationSinkTypeArray(sinkTypes));
    existingSettings.setSlackSettings(new SlackNotificationSettings().setUserHandle(handle));
  }

  /** Returns true if the event should be processed, false otherwise. */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return SUPPORTED_ENTITY_TYPES.contains(event.getEntityType())
        && SUPPORTED_ASPECT_TYPES.contains(event.getAspectName())
        && !event.getChangeType().equals(ChangeType.DELETE);
  }
}
