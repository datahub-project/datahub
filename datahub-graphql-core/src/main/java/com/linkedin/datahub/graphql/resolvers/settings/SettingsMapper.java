package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;

import com.datahub.authorization.AuthUtil;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailIntegrationSettings;
import com.linkedin.datahub.graphql.generated.GlobalIntegrationSettings;
import com.linkedin.datahub.graphql.generated.GlobalNotificationSettings;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.datahub.graphql.generated.GlobalVisualSettings;
import com.linkedin.datahub.graphql.generated.HelpLink;
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
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Utility functions useful for Settings resolvers. */
public class SettingsMapper {

  private final SecretService _secretService;

  public SettingsMapper(final SecretService secretService) {
    _secretService = secretService;
  }

  /** Returns true if the authenticated user is able to manage global settings. */
  public static boolean canManageGlobalSettings(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_GLOBAL_SETTINGS);
  }

  public static GlobalSettingsInfo getGlobalSettings(
      @Nonnull OperationContext opContext, final EntityClient entityClient) {
    try {
      final EntityResponse entityResponse =
          entityClient.getV2(
              opContext,
              Constants.GLOBAL_SETTINGS_ENTITY_NAME,
              Constants.GLOBAL_SETTINGS_URN,
              ImmutableSet.of(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME));

      if (entityResponse == null
          || !entityResponse.getAspects().containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        throw new RuntimeException(
            "Failed to retrieve global settings! Global settings not found, but are required.");
      }

      return new GlobalSettingsInfo(
          entityResponse
              .getAspects()
              .get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)
              .getValue()
              .data());
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve global settings!", e);
    }
  }

  /** Maps GMS settings into GraphQL global settings. */
  public GlobalSettings mapGlobalSettings(
      @Nonnull final QueryContext context, @Nonnull GlobalSettingsInfo input) {
    final GlobalSettings result = new GlobalSettings();
    result.setIntegrationSettings(mapGlobalIntegrationSettings(input.getIntegrations()));
    result.setNotificationSettings(
        mapGlobalNotificationSettings(context, input.getNotifications()));
    if (input.hasSso() && input.getSso() != null) {
      result.setSsoSettings(mapSsoSettings(input.getSso()));
    }
    if (input.hasVisual() && input.getVisual() != null) {
      result.setVisualSettings(mapVisualSettings(input.getVisual()));
    }
    return result;
  }

  private GlobalIntegrationSettings mapGlobalIntegrationSettings(
      @Nonnull com.linkedin.settings.global.GlobalIntegrationSettings input) {
    final GlobalIntegrationSettings result = new GlobalIntegrationSettings();
    if (input.hasSlackSettings()) {
      result.setSlackSettings(mapSlackIntegrationSettings(input.getSlackSettings()));
    }
    if (input.hasEmailSettings()) {
      result.setEmailSettings(mapEmailIntegrationSettings(input.getEmailSettings()));
    }
    return result;
  }

  private SlackIntegrationSettings mapSlackIntegrationSettings(
      @Nonnull com.linkedin.settings.global.SlackIntegrationSettings input) {
    final SlackIntegrationSettings result = new SlackIntegrationSettings();
    if (input.hasDefaultChannelName()) {
      result.setDefaultChannelName(input.getDefaultChannelName());
    }
    // Legacy
    if (input.hasEncryptedBotToken()) {
      result.setBotToken(_secretService.decrypt(input.getEncryptedBotToken()));
    }
    return result;
  }

  private EmailIntegrationSettings mapEmailIntegrationSettings(
      @Nonnull com.linkedin.settings.global.EmailIntegrationSettings input) {
    final EmailIntegrationSettings result = new EmailIntegrationSettings();
    result.setDefaultEmail(input.getDefaultEmail(GetMode.NULL));
    return result;
  }

  private GlobalNotificationSettings mapGlobalNotificationSettings(
      @Nonnull final QueryContext context,
      @Nonnull com.linkedin.settings.global.GlobalNotificationSettings input) {
    final GlobalNotificationSettings result = new GlobalNotificationSettings();
    if (input.hasSettings()) {
      result.setSettings(mapNotificationSettings(context, input.getSettings()));
    } else {
      result.setSettings(Collections.emptyList());
    }
    return result;
  }

  private static List<NotificationSetting> mapNotificationSettings(
      @Nonnull final QueryContext context, NotificationSettingMap settings) {
    final List<NotificationSetting> result = new ArrayList<>();
    settings.forEach((key, value) -> result.add(mapNotificationSetting(context, key, value)));
    return result;
  }

  private static NotificationSetting mapNotificationSetting(
      @Nonnull final QueryContext context,
      String typeStr,
      com.linkedin.settings.NotificationSetting setting) {
    final NotificationSetting result = new NotificationSetting();
    result.setType(NotificationScenarioType.valueOf(typeStr));
    result.setValue(NotificationSettingValue.valueOf(setting.getValue().name()));
    if (setting.hasParams()) {
      result.setParams(StringMapMapper.map(context, setting.getParams()));
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

  private GlobalVisualSettings mapVisualSettings(
      @Nonnull com.linkedin.settings.global.GlobalVisualSettings gmsGlobalVisualSettings) {
    final GlobalVisualSettings result = new GlobalVisualSettings();
    if (gmsGlobalVisualSettings.hasHelpLink()) {
      HelpLink helpLink = new HelpLink();
      helpLink.setIsEnabled((gmsGlobalVisualSettings.getHelpLink().isIsEnabled()));
      helpLink.setLabel((gmsGlobalVisualSettings.getHelpLink().getLabel()));
      helpLink.setLink((gmsGlobalVisualSettings.getHelpLink().getLink()));
      result.setHelpLink(helpLink);
    }

    if (gmsGlobalVisualSettings.hasCustomLogoUrl()) {
      result.setCustomLogoUrl(gmsGlobalVisualSettings.getCustomLogoUrl());
    }
    if (gmsGlobalVisualSettings.hasCustomOrgName()) {
      result.setCustomOrgName(gmsGlobalVisualSettings.getCustomOrgName());
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
    if (input.hasUserNameClaimRegex()) {
      result.setUserNameClaimRegex(input.getUserNameClaimRegex());
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
    if (input.hasExtractGroupsEnabled()) {
      result.setExtractGroupsEnabled(input.isExtractGroupsEnabled());
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
    if (input.hasPreferredJwsAlgorithm2()) {
      result.setPreferredJwsAlgorithm(input.getPreferredJwsAlgorithm2());
    }
    return result;
  }
}
