package com.linkedin.metadata.kafka.hook.user;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.settings.SettingsServiceFactory;
import com.linkedin.gms.factory.subscription.SubscriptionServiceFactory;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.platformresource.PlatformResourceInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/** Initializes a corpUser's aspects with values such as slack member ID when they sign up. */
@Slf4j
@Component
@Import({
  SubscriptionServiceFactory.class,
  SettingsServiceFactory.class,
  SystemAuthenticationFactory.class
})
public class SlackMemberResolutionHook implements MetadataChangeLogHook {
  private static final Set<String> SUPPORTED_ENTITY_TYPES = ImmutableSet.of(CORP_USER_ENTITY_NAME);

  /**
   * NOTE: we listen to corpUserSettings because it's synced in from all other aspects via {@link
   * com.linkedin.metadata.kafka.hook.notification.settings.DefaultNotificationSettingsHook}
   */
  private static final Set<String> SUPPORTED_ASPECT_TYPES =
      ImmutableSet.of(CORP_USER_SETTINGS_ASPECT_NAME);

  private final SystemEntityClient entityClient;
  @Getter private final String consumerGroupSuffix;
  private OperationContext systemOperationContext;
  private final SlackMemberResolutionUtils memberResolutionUtils;
  @Getter private final boolean isEnabled;

  @Autowired
  public SlackMemberResolutionHook(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull @Value("${corpuser.hook.slackMemberResolutionHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${corpuser.hook.slackMemberResolutionHook.consumerGroupSuffix}")
          String consumerGroupSuffix) {

    this.entityClient = Objects.requireNonNull(entityClient, "entityService must not be null");
    this.memberResolutionUtils = new SlackMemberResolutionUtils(entityClient);
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public SlackMemberResolutionHook(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final Boolean isEnabled,
      @Nonnull final String consumerGroupSuffix,
      @Nonnull final SlackMemberResolutionUtils memberResolutionUtils) {

    this.entityClient = Objects.requireNonNull(entityClient, "entityService must not be null");
    this.memberResolutionUtils = memberResolutionUtils;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @Override
  public SlackMemberResolutionHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized Owner Subscription hook");
    return this;
  }

  /**
   * Returns true if the event should be processed, false otherwise. This hook only processes
   * corpUserInfo creation events.
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return SUPPORTED_ENTITY_TYPES.contains(event.getEntityType())
        && !event.getChangeType().equals(ChangeType.DELETE)
        && SUPPORTED_ASPECT_TYPES.contains(event.getAspectName());
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (!this.isEnabled || !isEligibleForProcessing(event)) {
      return;
    }
    if (!event.hasAspect() || !event.hasAspectName()) {
      log.warn(
          "SlackMemberResolutionHook received an event without an aspect or aspectName. Skipping.");
      return;
    }
    if (event.getAspectName().equals(CORP_USER_SETTINGS_ASPECT_NAME)) {
      log.info(
          String.format(
              "Found corpUserSettings update event for %s. Attempting to resolve and hydrate corpUserSettings slack member details.",
              event.getEntityUrn()));
      tryHydrateSlackMemberDetails(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              CorpUserSettings.class));
    } else {
      log.warn(
          String.format(
              "Aspect type %s did not match expected aspects for SlackMemberResolutionHook. No-op.",
              event.getAspectName()));
    }
  }

  private void tryHydrateSlackMemberDetails(
      @Nonnull Urn userUrn, @Nonnull CorpUserSettings userSettings) {
    // 1. Check if slack member info is already stored in the corpUserInfo
    final CorpUserEditableInfo corpUserEditableInfo;
    try {
      corpUserEditableInfo =
          memberResolutionUtils.fetchCorpUserEditableInfo(this.systemOperationContext, userUrn);
    } catch (Exception e) {
      log.error(
          String.format(
              "Encountered an exception when getting corpUserEditableInfo for user %s. Ending hook early.",
              userUrn),
          e);
      return;
    }
    if (corpUserEditableInfo == null) {
      log.debug(
          String.format(
              "No corpUserEditableInfo aspect for user %s. Will create a new aspect.", userUrn));
    } else if (corpUserEditableInfo.hasSlack() && !corpUserEditableInfo.getSlack().isEmpty()) {
      // NOTE: {@link
      // com.linkedin.metadata.kafka.hook.notification.settings.DefaultNotificationSettingsHook}
      // will
      // automatically copy slack member info from the user's profile into their notification
      // settings
      log.debug(
          String.format(
              "User %s has slack already defined: %s. Skipping resolution.",
              userUrn, corpUserEditableInfo.getSlack()));
      return;
    }

    // 2. Resolve with memberId if set in notification settings
    final boolean isResolvedWithSlackNotificationSettings =
        tryResolveWithSlackNotificationSettings(userUrn, userSettings, corpUserEditableInfo);
    if (isResolvedWithSlackNotificationSettings) {
      log.info(
          String.format(
              "Successfully hydrated slackMember in corpUserEditableInfo with slackNotificationSettings for user %s",
              userUrn));
      return;
    }

    // 3. Resolve with email if set in notification settings
    final boolean isResolvedWithEmailNotificationSettings =
        tryResolveWithEmailNotificationSettings(userUrn, userSettings, corpUserEditableInfo);
    if (isResolvedWithEmailNotificationSettings) {
      log.info(
          String.format(
              "Successfully hydrated slackMember in corpUserEditableInfo with emailNotificationSettings for user %s",
              userUrn));
    }
  }

  private boolean tryResolveWithSlackNotificationSettings(
      @Nonnull Urn userUrn,
      @Nonnull CorpUserSettings userSettings,
      @Nullable CorpUserEditableInfo existingEditableInfo) {
    // 1. Check if we have Slack in settings
    if (!userSettings.hasNotificationSettings()
        || !userSettings.getNotificationSettings().hasSlackSettings()) {
      log.debug(
          String.format(
              "User settings aspect for user %s does not have slack settings defined within there",
              userUrn));
      return false;
    }
    final SlackNotificationSettings notificationSettings =
        userSettings.getNotificationSettings().getSlackSettings();
    if (!notificationSettings.hasUserHandle()) {
      log.debug(
          String.format(
              "Slack settings for user %s does not have userHandle set within there", userUrn));
      return false;
    }
    // 2. Check if we have Slack membership info for this user stored
    final String slackHandle = notificationSettings.getUserHandle();
    final PlatformResourceInfo matchingPlatformResource;
    try {
      matchingPlatformResource =
          memberResolutionUtils.findSlackMemberWithEmailOrMemberID(
              this.systemOperationContext, slackHandle);
    } catch (Exception e) {
      log.error(
          String.format(
              "Encountered an exception when searching for slack member PlatformResource for user %s with slack handle %s",
              userUrn, slackHandle),
          e);
      return false;
    }

    if (matchingPlatformResource == null) {
      log.debug(
          String.format(
              "No Slack member platform resources found for user %s with slack handle %s. Completing hook.",
              userUrn, slackHandle));
      return false;
    }

    // 3. Update user profile with slack member info
    // NOTE: {@link
    // com.linkedin.metadata.kafka.hook.notification.settings.DefaultNotificationSettingsHook} will
    // automatically copy slack member info from the user's profile into their notification settings
    log.debug(
        String.format(
            "Found member resource matching user %s slack handle %s. Updating corpUserEditableInfo",
            userUrn, slackHandle));
    try {
      return memberResolutionUtils.upsertCorpUserAspectsWithSlackMemberDetails(
          systemOperationContext, userUrn, matchingPlatformResource, existingEditableInfo);
    } catch (Exception e) {
      log.error(
          String.format(
              "Encountered an exception while trying to upsert corpUserEditableInfo with slack member details, %s",
              userUrn),
          e);
      return false;
    }
  }

  private boolean tryResolveWithEmailNotificationSettings(
      @Nonnull Urn userUrn,
      @Nonnull CorpUserSettings userSettings,
      @Nullable CorpUserEditableInfo existingEditableInfo) {
    // 1. Check if we have email in settings
    if (!userSettings.hasNotificationSettings()
        || !userSettings.getNotificationSettings().hasEmailSettings()) {
      log.debug(
          String.format(
              "User settings aspect for user %s does not have email settings defined within there",
              userUrn));
      return false;
    }
    final EmailNotificationSettings emailNotificationSettings =
        userSettings.getNotificationSettings().getEmailSettings();
    if (!emailNotificationSettings.hasEmail()) {
      log.debug(
          String.format(
              "Email settings for user %s does not have email set within there", userUrn));
      return false;
    }
    // 2. Check if we have Slack membership info for this user stored
    final String email = emailNotificationSettings.getEmail();
    final PlatformResourceInfo matchingPlatformResource;
    try {
      matchingPlatformResource =
          memberResolutionUtils.findSlackMemberWithEmailOrMemberID(systemOperationContext, email);
    } catch (Exception e) {
      log.error(
          String.format(
              "Encountered an exception when searching for slack member PlatformResource for user %s with email %s",
              userUrn, email),
          e);
      return false;
    }

    if (matchingPlatformResource == null) {
      log.debug(
          String.format(
              "No Slack member platform resources found for user %s with email %s. Completing hook.",
              userUrn, email));
      return false;
    }

    // 3. Update user profile with slack member info
    // NOTE: {@link
    // com.linkedin.metadata.kafka.hook.notification.settings.DefaultNotificationSettingsHook} will
    // automatically copy slack member info from the user's profile into their notification settings
    log.debug(
        String.format(
            "Found member resource matching user %s email %s. Updating corpUserEditableInfo",
            userUrn, email));
    try {
      return memberResolutionUtils.upsertCorpUserAspectsWithSlackMemberDetails(
          systemOperationContext, userUrn, matchingPlatformResource, existingEditableInfo);
    } catch (Exception e) {
      log.error(
          String.format(
              "Encountered an exception while trying to upsert corpUserEditableInfo with slack member details, %s",
              userUrn),
          e);
      return false;
    }
  }
}
