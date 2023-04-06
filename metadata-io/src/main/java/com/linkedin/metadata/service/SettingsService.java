package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.ActorType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;


/**
 * This class is used to permit easy CRUD operations on both <b>Global</b> and <b>Personal</b>
 * DataHub settings.
 *
 * Note that no Authorization is performed within the service. The expectation
 * is that the caller has already verified the permissions of the active Actor.
 */
@Slf4j
public class SettingsService extends BaseService {
  public static final CorpUserSettings DEFAULT_CORP_USER_SETTINGS = new CorpUserSettings()
      .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));

  public SettingsService(@Nonnull final EntityClient entityClient, @Nonnull final Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Returns the settings for a particular user, or null if they do not exist yet.
   *
   * @param userUrn the urn of the user to fetch settings for
   * @param authentication the current authentication
   *
   * @return an instance of {@link CorpUserSettings} for the specified user, or null if none exists.
   */
  @Nullable
  public CorpUserSettings getCorpUserSettings(
      @Nonnull final Urn userUrn,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      if (!entityClient.exists(userUrn, authentication)) {
        throw new RuntimeException(String.format("User %s does not exist", userUrn));
      }

      EntityResponse response = this.entityClient.getV2(
          CORP_USER_ENTITY_NAME,
          userUrn,
          ImmutableSet.of(CORP_USER_SETTINGS_ASPECT_NAME),
          authentication
      );
      if (response != null && response.getAspects().containsKey(Constants.CORP_USER_SETTINGS_ASPECT_NAME)) {
        return new CorpUserSettings(getDataMapFromEntityResponse(response, CORP_USER_SETTINGS_ASPECT_NAME));
      }
      // No aspect found
      return null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get CorpUserSettings for user with urn %s", userUrn), e);
    }
  }

  /**
   * Returns the settings for a particular group, or null if they do not exist yet.
   *
   * @param groupUrn the urn of the grouo to fetch settings for
   * @param authentication the current authentication
   *
   * @return an instance of {@link CorpGroupSettings} for the specified group or null if none exists.
   */
  @Nullable
  public CorpGroupSettings getCorpGroupSettings(
      @Nonnull final Urn groupUrn,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      if (!entityClient.exists(groupUrn, authentication)) {
        throw new RuntimeException(String.format("Group %s does not exist", groupUrn));
      }

      final EntityResponse response = this.entityClient.getV2(
          CORP_GROUP_ENTITY_NAME,
          groupUrn,
          ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME),
          authentication
      );
      if (response != null && response.getAspects().containsKey(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME)) {
        return new CorpGroupSettings(getDataMapFromEntityResponse(response, CORP_GROUP_SETTINGS_ASPECT_NAME));
      }
      // No aspect found
      return null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get CorpGroupSettings for group with urn %s", groupUrn), e);
    }
  }

  /**
   * Updates the settings for a given user.
   *
   * Note that this method does not do authorization validation.
   * It is assumed that users of this class have already authorized the operation.
   *
   * @param userUrn the urn of the user
   * @param authentication the current authentication
   */
  public void updateCorpUserSettings(
      @Nonnull final Urn userUrn,
      @Nonnull final CorpUserSettings newSettings,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(userUrn, "userUrn must not be null");
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      if (!entityClient.exists(userUrn, authentication)) {
        throw new RuntimeException(String.format("User %s does not exist", userUrn));
      }

      MetadataChangeProposal proposal = AspectUtils.buildMetadataChangeProposal(
          userUrn,
          CORP_USER_SETTINGS_ASPECT_NAME,
          newSettings);
      this.entityClient.ingestProposal(proposal, authentication, true);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update CorpUserSettings for user with urn %s", userUrn), e);
    }
  }

  /**
   * Updates the settings for a given group.
   *
   * Note that this method does not do authorization validation.
   * It is assumed that users of this class have already authorized the operation.
   *
   * @param groupUrn the urn of the group
   * @param authentication the current authentication
   */
  public void updateCorpGroupSettings(
      @Nonnull final Urn groupUrn,
      @Nonnull final CorpGroupSettings newSettings,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      if (!entityClient.exists(groupUrn, authentication)) {
        throw new RuntimeException(String.format("Group %s does not exist", groupUrn));
      }

      MetadataChangeProposal proposal = AspectUtils.buildMetadataChangeProposal(
          groupUrn,
          CORP_GROUP_SETTINGS_ASPECT_NAME,
          newSettings);
      this.entityClient.ingestProposal(proposal, authentication, true);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update CorpGroupSettings for group with urn %s", groupUrn),
          e);
    }
  }

  @Nonnull
  public SlackNotificationSettings createSlackNotificationSettings(@Nullable String userHandle,
      @Nullable List<String> channels) {
    if (userHandle == null && channels == null) {
      throw new RuntimeException("User handle and channels cannot both be null");
    }

    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    if (userHandle != null) {
      slackNotificationSettings.setUserHandle(userHandle);
    }
    if (channels != null) {
      slackNotificationSettings.setChannels(new StringArray(channels));
    }
    return slackNotificationSettings;
  }

  /**
   * Returns the Global Settings. They are expected to exist.
   *
   * @param authentication the current authentication
   * @return an instance of {@link GlobalSettingsInfo}, or null if none exists.
   */
  public GlobalSettingsInfo getGlobalSettings(@Nonnull final Authentication authentication) {
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      EntityResponse response = this.entityClient.getV2(
          GLOBAL_SETTINGS_ENTITY_NAME,
          GLOBAL_SETTINGS_URN,
          ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME),
          authentication
      );
      if (response != null && response.getAspects().containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        return new GlobalSettingsInfo(response.getAspects().get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME).getValue().data());
      }
      // No aspect found
      log.warn("Failed to retrieve Global Settings. No settings exist, but they should. Returning null");
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Global Settings!", e);
    }
  }

  /**
   * Updates the Global settings.
   *
   * This performs a read-modify-write of the underlying GlobalSettingsInfo aspect.
   *
   * Note that this method does not do authorization validation.
   * It is assumed that users of this class have already authorized the operation.
   *
   * @param newSettings the new value for the global settings.
   * @param authentication the current authentication
   */
  public void updateGlobalSettings(
      @Nonnull final GlobalSettingsInfo newSettings,
      @Nonnull final Authentication authentication) {
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      MetadataChangeProposal proposal = AspectUtils.buildMetadataChangeProposal(
          GLOBAL_SETTINGS_URN,
          GLOBAL_SETTINGS_INFO_ASPECT_NAME,
          newSettings);
      this.entityClient.ingestProposal(proposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to update Global settings", e);
    }
  }
}