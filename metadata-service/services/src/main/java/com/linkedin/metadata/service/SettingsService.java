package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on both <b>Global</b> and <b>Personal</b>
 * DataHub settings.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class SettingsService extends BaseService {
  public static final CorpUserSettings DEFAULT_CORP_USER_SETTINGS =
      new CorpUserSettings()
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));

  public SettingsService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  /**
   * Returns the settings for a group of users, or empty map if none exist.
   *
   * @param opContext operation's context
   * @param users the urns of the users to fetch settings for
   * @return a map of urn to {@link CorpUserSettings} for the specified users.
   */
  @Nonnull
  public Map<Urn, CorpUserSettings> batchGetCorpUserSettings(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> users) {
    Objects.requireNonNull(users, "users must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      Map<Urn, EntityResponse> response =
          this.entityClient.batchGetV2(
              opContext,
              CORP_USER_ENTITY_NAME,
              new HashSet<>(users),
              ImmutableSet.of(CORP_USER_SETTINGS_ASPECT_NAME));

      final Map<Urn, CorpUserSettings> userSettingsMap = new HashMap<>();
      for (final Urn user : users) {
        if (response.containsKey(user)
            && response
                .get(user)
                .getAspects()
                .containsKey(Constants.CORP_USER_SETTINGS_ASPECT_NAME)) {
          userSettingsMap.put(
              user,
              new CorpUserSettings(
                  getDataMapFromEntityResponse(
                      response.get(user), CORP_USER_SETTINGS_ASPECT_NAME)));
        }
      }
      return userSettingsMap;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve batch of corp user settings for users with urns %s", users),
          e);
    }
  }

  /**
   * Returns the settings for a group of groups, or empty map if none exist.
   *
   * @param opContext operation's context
   * @param groups the urns of the groups to fetch settings for
   * @return a map of urn to {@link CorpGroupSettings} for the specified groups.
   */
  @Nonnull
  public Map<Urn, CorpGroupSettings> batchGetCorpGroupSettings(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> groups) {
    Objects.requireNonNull(groups, "groups must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      Map<Urn, EntityResponse> response =
          this.entityClient.batchGetV2(
              opContext,
              CORP_GROUP_ENTITY_NAME,
              new HashSet<>(groups),
              ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME));

      final Map<Urn, CorpGroupSettings> groupSettingsMap = new HashMap<>();
      for (final Urn group : groups) {
        if (response.containsKey(group)
            && response
                .get(group)
                .getAspects()
                .containsKey(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME)) {
          groupSettingsMap.put(
              group,
              new CorpGroupSettings(
                  getDataMapFromEntityResponse(
                      response.get(group), CORP_GROUP_SETTINGS_ASPECT_NAME)));
        }
      }
      return groupSettingsMap;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve batch of corp group settings for groups with urns %s", groups),
          e);
    }
  }

  /**
   * Returns the settings for a particular user, or null if they do not exist yet.
   *
   * @param opContext operation's context
   * @param user the urn of the user to fetch settings for
   * @return an instance of {@link CorpUserSettings} for the specified user, or null if none exists.
   */
  @Nullable
  public CorpUserSettings getCorpUserSettings(
      @Nonnull OperationContext opContext, @Nonnull final Urn user) {
    Objects.requireNonNull(user, "user must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      if (!entityClient.exists(opContext, user)) {
        throw new RuntimeException(String.format("User %s does not exist", user));
      }

      EntityResponse response =
          this.entityClient.getV2(
              opContext,
              CORP_USER_ENTITY_NAME,
              user,
              ImmutableSet.of(CORP_USER_SETTINGS_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.CORP_USER_SETTINGS_ASPECT_NAME)) {
        return new CorpUserSettings(
            getDataMapFromEntityResponse(response, CORP_USER_SETTINGS_ASPECT_NAME));
      }
      // No aspect found.
      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get CorpUserSettings for user with urn %s", user), e);
    }
  }

  /**
   * Returns the settings for a particular group, or null if they do not exist yet.
   *
   * @param opContext operation's context
   * @param groupUrn the urn of the grouo to fetch settings for
   * @return an instance of {@link CorpGroupSettings} for the specified group or null if none
   *     exists.
   */
  @Nullable
  public CorpGroupSettings getCorpGroupSettings(
      @Nonnull OperationContext opContext, @Nonnull final Urn groupUrn) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      if (!entityClient.exists(opContext, groupUrn)) {
        throw new RuntimeException(String.format("Group %s does not exist", groupUrn));
      }

      final EntityResponse response =
          this.entityClient.getV2(
              opContext,
              CORP_GROUP_ENTITY_NAME,
              groupUrn,
              ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME)) {
        return new CorpGroupSettings(
            getDataMapFromEntityResponse(response, CORP_GROUP_SETTINGS_ASPECT_NAME));
      }
      // No aspect found
      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get CorpGroupSettings for group with urn %s", groupUrn), e);
    }
  }

  /**
   * Updates the settings for a given user.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param opContext operation's context
   * @param user the urn of the user
   * @param newSettings the new settings to apply
   */
  public void updateCorpUserSettings(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn user,
      @Nonnull final CorpUserSettings newSettings) {
    Objects.requireNonNull(user, "userUrn must not be null");
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      if (!entityClient.exists(opContext, user)) {
        throw new RuntimeException(String.format("User %s does not exist", user));
      }

      MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(
              user, CORP_USER_SETTINGS_ASPECT_NAME, newSettings);
      this.entityClient.ingestProposal(opContext, proposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update CorpUserSettings for user with urn %s", user), e);
    }
  }

  /**
   * Updates the settings for a given group.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param opContext operation's context
   * @param groupUrn the urn of the group
   * @param newSettings the new settings to apply
   */
  public void updateCorpGroupSettings(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn groupUrn,
      @Nonnull final CorpGroupSettings newSettings) {
    Objects.requireNonNull(groupUrn, "groupUrn must not be null");
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      if (!entityClient.exists(opContext, groupUrn)) {
        throw new RuntimeException(String.format("Group %s does not exist", groupUrn));
      }

      MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(
              groupUrn, CORP_GROUP_SETTINGS_ASPECT_NAME, newSettings);
      this.entityClient.ingestProposal(opContext, proposal, true);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update CorpGroupSettings for group with urn %s", groupUrn), e);
    }
  }

  @Nonnull
  public SlackNotificationSettings createSlackNotificationSettings(
      @Nullable String userHandle, @Nullable List<String> channels) {
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    if (userHandle != null) {
      slackNotificationSettings.setUserHandle(userHandle);
    }
    if (channels != null) {
      slackNotificationSettings.setChannels(new StringArray(channels));
    }
    return slackNotificationSettings;
  }

  @Nonnull
  public EmailNotificationSettings createEmailNotificationSettings(@Nonnull final String email) {
    final EmailNotificationSettings emailNotificationSettings = new EmailNotificationSettings();
    emailNotificationSettings.setEmail(email);
    return emailNotificationSettings;
  }

  /**
   * Returns the Global Settings. They are expected to exist.
   *
   * @param opContext operation's context
   * @return an instance of {@link GlobalSettingsInfo}, or null if none exists.
   */
  public GlobalSettingsInfo getGlobalSettings(@Nonnull OperationContext opContext) {
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      EntityResponse response =
          this.entityClient.getV2(
              opContext,
              GLOBAL_SETTINGS_ENTITY_NAME,
              GLOBAL_SETTINGS_URN,
              ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        return new GlobalSettingsInfo(
            response
                .getAspects()
                .get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)
                .getValue()
                .data());
      }
      // No aspect found
      log.warn(
          "Failed to retrieve Global Settings. No settings exist, but they should. Returning null");
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Global Settings!", e);
    }
  }

  /**
   * Updates the Global settings.
   *
   * <p>This performs a read-modify-write of the underlying GlobalSettingsInfo aspect.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param opContext operation's context
   * @param newSettings the new value for the global settings.
   */
  public void updateGlobalSettings(
      @Nonnull OperationContext opContext, @Nonnull final GlobalSettingsInfo newSettings) {
    Objects.requireNonNull(newSettings, "newSettings must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(
              GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, newSettings);
      this.entityClient.ingestProposal(opContext, proposal, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to update Global settings", e);
    }
  }

  public EntityClient getEntityClient() {
    return this.entityClient;
  }
}
