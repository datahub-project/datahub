package com.linkedin.datahub.graphql.resolvers.settings;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlobalIntegrationSettings;
import com.linkedin.datahub.graphql.generated.GlobalNotificationSettings;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.OidcSettings;
import com.linkedin.datahub.graphql.generated.SlackIntegrationSettings;
import com.linkedin.datahub.graphql.generated.SsoSettings;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;


/**
 * Utility functions useful for Settings resolvers.
 */
public class SettingsMapper {

  private final SecretService _secretService;

  public SettingsMapper(final SecretService secretService) {
    _secretService = secretService;
  }

  /**
   * Returns true if the authenticated user is able to manage global settings.
   */
  public static boolean canManageGlobalSettings(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOBAL_SETTINGS);
  }

  public static GlobalSettingsInfo getGlobalSettings(final EntityClient entityClient,
      final Authentication authentication) {
    try {
      final EntityResponse entityResponse =
          entityClient.getV2(Constants.GLOBAL_SETTINGS_ENTITY_NAME, Constants.GLOBAL_SETTINGS_URN,
              ImmutableSet.of(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME), authentication);

      if (entityResponse == null || !entityResponse.getAspects()
          .containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        throw new RuntimeException("Failed to retrieve global settings! Global settings not found, but are required.");
      }

      return new GlobalSettingsInfo(
          entityResponse.getAspects().get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME).getValue().data());
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve global settings!", e);
    }
  }

  /**
   * Maps GMS settings into GraphQL global settings.
   */
  public GlobalSettings mapGlobalSettings(@Nonnull GlobalSettingsInfo input) {
    final GlobalSettings result = new GlobalSettings();
    result.setIntegrationSettings(mapGlobalIntegrationSettings(input.getIntegrations()));
    result.setNotificationSettings(mapGlobalNotificationSettings(input.getNotifications()));
    if (input.hasSso() && input.getSso() != null) {
      result.setSsoSettings(mapSsoSettings(input.getSso()));
    }
    return result;
  }

  private GlobalIntegrationSettings mapGlobalIntegrationSettings(
      @Nonnull com.linkedin.settings.global.GlobalIntegrationSettings input) {
    final GlobalIntegrationSettings result = new GlobalIntegrationSettings();
    if (input.hasSlackSettings()) {
      result.setSlackSettings(mapSlackIntegrationSettings(input.getSlackSettings()));
    }
    return result;
  }

  private SlackIntegrationSettings mapSlackIntegrationSettings(
      @Nonnull com.linkedin.settings.global.SlackIntegrationSettings input) {
    final SlackIntegrationSettings result = new SlackIntegrationSettings();
    result.setEnabled(input.isEnabled());
    if (input.hasDefaultChannelName()) {
      result.setDefaultChannelName(input.getDefaultChannelName());
    }
    if (input.hasEncryptedBotToken()) {
      result.setBotToken(_secretService.decrypt(input.getEncryptedBotToken()));
    }
    return result;
  }

  private GlobalNotificationSettings mapGlobalNotificationSettings(
      @Nonnull com.linkedin.settings.global.GlobalNotificationSettings input) {
    final GlobalNotificationSettings result = new GlobalNotificationSettings();
    if (input.hasSettings()) {
      result.setSettings(mapNotificationSettings(input.getSettings()));
    } else {
      result.setSettings(Collections.emptyList());
    }
    return result;
  }

  private List<NotificationSetting> mapNotificationSettings(NotificationSettingMap settings) {
    final List<NotificationSetting> result = new ArrayList<>();
    settings.forEach((key, value) -> result.add(mapNotificationSetting(key, value)));
    return result;
  }

  private NotificationSetting mapNotificationSetting(String typeStr,
      com.linkedin.settings.NotificationSetting setting) {
    final NotificationSetting result = new NotificationSetting();
    result.setType(NotificationScenarioType.valueOf(typeStr));
    result.setValue(NotificationSettingValue.valueOf(setting.getValue().name()));
    if (setting.hasParams()) {
      result.setParams(StringMapMapper.map(setting.getParams()));
    }
    return result;
  }

  private SsoSettings mapSsoSettings(@Nonnull com.linkedin.settings.global.SsoSettings input) {
    final SsoSettings result = new SsoSettings();
    if (input.hasOidcSettings()) {
      result.setOidcSettings(mapOidcSettings(input.getOidcSettings()));
    }
    return result;
  }

  private OidcSettings mapOidcSettings(@Nonnull com.linkedin.settings.global.OidcSettings input) {
    final OidcSettings result = new OidcSettings();
    result.setEnabled(input.isEnabled());
    result.setClientId(input.getClientId());
    result.setClientSecret(_secretService.decrypt(input.getClientSecret()));
    result.setDiscoveryUri(input.getDiscoveryUri());
    if (input.hasUserNameClaim()) {
      result.setUserNameClaim(input.getUserNameClaim());
    }
    if (input.hasScope()) {
      result.setScope(input.getScope());
    }
    if (input.hasClientAuthenticationMethod()) {
      result.setClientAuthenticationMethod(input.getClientAuthenticationMethod());
    }
    if (input.hasJitProvisioningEnabled()) {
      result.setJitProvisioningEnabled(input.isJitProvisioningEnabled());
    }
    if (input.hasPreProvisioningRequired()) {
      result.setPreProvisioningRequired(input.isPreProvisioningRequired());
    }
    if (input.hasGroupsClaim()) {
      result.setGroupsClaim(input.getGroupsClaim());
    }
    if (input.hasResponseType()) {
      result.setResponseType(input.getResponseType());
    }
    if (input.hasResponseMode()) {
      result.setResponseMode(input.getResponseMode());
    }
    if (input.hasUseNonce()) {
      result.setUseNonce(input.isUseNonce());
    }
    if (input.hasReadTimeout()) {
      result.setReadTimeout(input.getReadTimeout());
    }
    if (input.hasExtractJwtAccessTokenClaims()) {
      result.setExtractJwtAccessTokenClaims(input.isExtractJwtAccessTokenClaims());
    }
    return result;
  }
}

