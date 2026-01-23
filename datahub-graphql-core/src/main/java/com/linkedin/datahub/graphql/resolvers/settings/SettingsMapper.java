package com.linkedin.datahub.graphql.resolvers.settings;

import com.datahub.authorization.AuthUtil;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AiAssistantSettings;
import com.linkedin.datahub.graphql.generated.AiInstruction;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import com.linkedin.datahub.graphql.generated.AiPluginAuthType;
import com.linkedin.datahub.graphql.generated.AiPluginConfig;
import com.linkedin.datahub.graphql.generated.AiPluginType;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.AuthInjectionLocation;
import com.linkedin.datahub.graphql.generated.DocumentationAiSettings;
import com.linkedin.datahub.graphql.generated.EmailIntegrationSettings;
import com.linkedin.datahub.graphql.generated.GlobalIntegrationSettings;
import com.linkedin.datahub.graphql.generated.GlobalNotificationSettings;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.datahub.graphql.generated.GlobalVisualSettings;
import com.linkedin.datahub.graphql.generated.HelpLink;
import com.linkedin.datahub.graphql.generated.MaintenanceSeverity;
import com.linkedin.datahub.graphql.generated.MaintenanceWindowSettings;
import com.linkedin.datahub.graphql.generated.OAuthAiPluginConfig;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.generated.OidcSettings;
import com.linkedin.datahub.graphql.generated.SampleDataSettings;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.generated.SharedApiKeyAiPluginConfig;
import com.linkedin.datahub.graphql.generated.SlackIntegrationSettings;
import com.linkedin.datahub.graphql.generated.SsoSettings;
import com.linkedin.datahub.graphql.generated.TeamsChannel;
import com.linkedin.datahub.graphql.generated.TeamsIntegrationSettings;
import com.linkedin.datahub.graphql.generated.UserApiKeyAiPluginConfig;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SampleDataStatus;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Utility functions useful for Settings resolvers. */
public class SettingsMapper {

  private final SecretService _secretService;
  private final FeatureFlags _featureFlags;

  public SettingsMapper(final SecretService secretService, final FeatureFlags featureFlags) {
    _secretService = secretService;
    _featureFlags = featureFlags;
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
    // Map Documentation AI Settings
    DocumentationAiSettings docAiSettings = mapDocumentationAiSettings(input);
    result.setDocumentationAi(docAiSettings);

    // Map AI Assistant Settings
    AiAssistantSettings aiAssistantSettings = mapAiAssistantSettings(input);
    result.setAiAssistant(aiAssistantSettings);

    // Map AI Plugins
    List<AiPluginConfig> aiPlugins = mapAiPlugins(input);
    result.setAiPlugins(aiPlugins);

    // Map Maintenance Window Settings
    if (input.hasMaintenanceWindow() && input.getMaintenanceWindow() != null) {
      result.setMaintenanceWindow(mapMaintenanceWindowSettings(input.getMaintenanceWindow()));
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
    if (input.hasTeamsSettings()) {
      result.setTeamsSettings(mapTeamsIntegrationSettings(input.getTeamsSettings()));
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

    // Resolve datahubAtMentionEnabled based on AI features, explicit setting, or feature flag
    // fallback.
    boolean isEnabled;
    if (!_featureFlags.isAiFeaturesEnabled()) {
      isEnabled = false;
    } else if (input.hasDatahubAtMentionEnabled()) {
      isEnabled = input.isDatahubAtMentionEnabled();
    } else {
      isEnabled = _featureFlags.isSlackAtMentionDefaultEnabled();
    }
    result.setDatahubAtMentionEnabled(isEnabled);

    return result;
  }

  private EmailIntegrationSettings mapEmailIntegrationSettings(
      @Nonnull com.linkedin.settings.global.EmailIntegrationSettings input) {
    final EmailIntegrationSettings result = new EmailIntegrationSettings();
    result.setDefaultEmail(input.getDefaultEmail(GetMode.NULL));
    return result;
  }

  private TeamsIntegrationSettings mapTeamsIntegrationSettings(
      @Nonnull com.linkedin.settings.global.TeamsIntegrationSettings input) {
    final TeamsIntegrationSettings result = new TeamsIntegrationSettings();
    if (input.hasDefaultChannel()) {
      result.setDefaultChannel(mapTeamsChannel(input.getDefaultChannel()));
    }
    return result;
  }

  private TeamsChannel mapTeamsChannel(@Nonnull com.linkedin.settings.global.TeamsChannel input) {
    final TeamsChannel result = new TeamsChannel();
    result.setId(input.getId());
    if (input.hasName()) {
      result.setName(input.getName(GetMode.NULL));
    }
    return result;
  }

  private GlobalNotificationSettings mapGlobalNotificationSettings(
      @Nonnull final QueryContext context,
      @Nonnull com.linkedin.settings.global.GlobalNotificationSettings input) {
    final GlobalNotificationSettings result = new GlobalNotificationSettings();
    if (input.hasSettings()) {
      result.setSettings(
          NotificationSettingMapMapper.mapNotificationSettings(context, input.getSettings()));
    } else {
      result.setSettings(Collections.emptyList());
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
    if (gmsGlobalVisualSettings.getSampleDataSettings() != null) {
      SampleDataSettings sampleDataSettings = new SampleDataSettings();
      sampleDataSettings.setEnabled(
          gmsGlobalVisualSettings
              .getSampleDataSettings()
              .getSampleDataStatus()
              .equals(SampleDataStatus.ENABLED));
      result.setSampleDataSettings(sampleDataSettings);
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

  private DocumentationAiSettings mapDocumentationAiSettings(@Nonnull GlobalSettingsInfo input) {
    DocumentationAiSettings docAiSettings = new DocumentationAiSettings();
    if (!_featureFlags.isAiFeaturesEnabled()) {
      docAiSettings.setEnabled(false);
      docAiSettings.setInstructions(Collections.emptyList());
    } else if (input.hasDocumentationAi() && input.getDocumentationAi() != null) {
      docAiSettings.setEnabled(input.getDocumentationAi().isEnabled());
      docAiSettings.setInstructions(
          mapAiInstructions(input.getDocumentationAi().getInstructions()));
    } else {
      docAiSettings.setEnabled(_featureFlags.isDocumentationAiDefaultEnabled());
      docAiSettings.setInstructions(Collections.emptyList());
    }
    return docAiSettings;
  }

  private AiAssistantSettings mapAiAssistantSettings(@Nonnull GlobalSettingsInfo input) {
    AiAssistantSettings aiAssistantSettings = new AiAssistantSettings();
    if (input.hasAiAssistant() && input.getAiAssistant() != null) {
      aiAssistantSettings.setInstructions(
          mapAiInstructions(input.getAiAssistant().getInstructions()));
    } else {
      aiAssistantSettings.setInstructions(Collections.emptyList());
    }
    return aiAssistantSettings;
  }

  private List<AiInstruction> mapAiInstructions(
      @Nonnull com.linkedin.settings.global.AiInstructionArray instructions) {
    return instructions.stream()
        .map(
            gmsInstruction -> {
              AiInstruction aiInstruction = new AiInstruction();
              aiInstruction.setId(gmsInstruction.getId());
              aiInstruction.setType(AiInstructionType.valueOf(gmsInstruction.getType().toString()));
              aiInstruction.setState(mapAiInstructionState(gmsInstruction.getState()));
              aiInstruction.setInstruction(gmsInstruction.getInstruction());
              aiInstruction.setCreated(mapAuditStamp(gmsInstruction.getCreated()));
              aiInstruction.setLastModified(mapAuditStamp(gmsInstruction.getLastModified()));
              return aiInstruction;
            })
        .collect(Collectors.toList());
  }

  private AiInstructionState mapAiInstructionState(
      @Nonnull com.linkedin.settings.global.AiInstructionState state) {
    switch (state) {
      case ACTIVE:
        return AiInstructionState.ACTIVE;
      case INACTIVE:
        return AiInstructionState.INACTIVE;
      default:
        throw new IllegalArgumentException("Unknown AiInstructionState: " + state);
    }
  }

  private AuditStamp mapAuditStamp(@Nonnull com.linkedin.common.AuditStamp auditStamp) {
    AuditStamp graphqlAuditStamp = new AuditStamp();
    graphqlAuditStamp.setTime(auditStamp.getTime());
    graphqlAuditStamp.setActor(auditStamp.getActor().toString());
    return graphqlAuditStamp;
  }

  private List<AiPluginConfig> mapAiPlugins(@Nonnull GlobalSettingsInfo input) {
    if (!input.hasAiPluginSettings()
        || input.getAiPluginSettings() == null
        || !input.getAiPluginSettings().hasPlugins()) {
      return Collections.emptyList();
    }

    List<AiPluginConfig> result = new ArrayList<>();
    for (com.linkedin.settings.global.AiPluginConfig gmsConfig :
        input.getAiPluginSettings().getPlugins()) {
      result.add(mapAiPluginConfig(gmsConfig));
    }
    return result;
  }

  private AiPluginConfig mapAiPluginConfig(
      @Nonnull com.linkedin.settings.global.AiPluginConfig gmsConfig) {
    AiPluginConfig result = new AiPluginConfig();
    result.setId(gmsConfig.getId());
    result.setType(AiPluginType.valueOf(gmsConfig.getType().toString()));
    result.setServiceUrn(gmsConfig.getServiceUrn().toString());
    result.setEnabled(gmsConfig.isEnabled());

    if (gmsConfig.hasInstructions()) {
      result.setInstructions(gmsConfig.getInstructions());
    }

    result.setAuthType(AiPluginAuthType.valueOf(gmsConfig.getAuthType().toString()));

    // Map oauthConfig (for USER_OAUTH)
    if (gmsConfig.hasOauthConfig()) {
      com.linkedin.settings.global.OAuthAiPluginConfig gmsOauthConfig = gmsConfig.getOauthConfig();
      OAuthAiPluginConfig oauthConfig = new OAuthAiPluginConfig();
      oauthConfig.setServerUrn(gmsOauthConfig.getServerUrn().toString());

      // Set partial OAuthAuthorizationServer for resolution by type resolver
      OAuthAuthorizationServer partialServer = new OAuthAuthorizationServer();
      partialServer.setUrn(gmsOauthConfig.getServerUrn().toString());
      oauthConfig.setServer(partialServer);

      if (gmsOauthConfig.hasRequiredScopes()) {
        oauthConfig.setRequiredScopes(new ArrayList<>(gmsOauthConfig.getRequiredScopes()));
      }

      result.setOauthConfig(oauthConfig);
    }

    // Map sharedApiKeyConfig (for SHARED_API_KEY)
    if (gmsConfig.hasSharedApiKeyConfig()) {
      com.linkedin.settings.global.SharedApiKeyAiPluginConfig gmsSharedConfig =
          gmsConfig.getSharedApiKeyConfig();
      if (gmsSharedConfig != null
          && gmsSharedConfig.hasCredentialUrn()
          && gmsSharedConfig.getCredentialUrn() != null) {
        SharedApiKeyAiPluginConfig sharedConfig = new SharedApiKeyAiPluginConfig();
        sharedConfig.setCredentialUrn(gmsSharedConfig.getCredentialUrn().toString());

        // Map auth injection settings with defaults matching PDL
        sharedConfig.setAuthLocation(
            gmsSharedConfig.hasAuthLocation()
                ? AuthInjectionLocation.valueOf(gmsSharedConfig.getAuthLocation().toString())
                : AuthInjectionLocation.HEADER);
        sharedConfig.setAuthHeaderName(
            gmsSharedConfig.hasAuthHeaderName()
                ? gmsSharedConfig.getAuthHeaderName()
                : "Authorization");
        if (gmsSharedConfig.hasAuthScheme()) {
          sharedConfig.setAuthScheme(gmsSharedConfig.getAuthScheme());
        }
        if (gmsSharedConfig.hasAuthQueryParam()) {
          sharedConfig.setAuthQueryParam(gmsSharedConfig.getAuthQueryParam());
        }

        result.setSharedApiKeyConfig(sharedConfig);
      }
    }

    // Map userApiKeyConfig (for USER_API_KEY)
    if (gmsConfig.hasUserApiKeyConfig()) {
      com.linkedin.settings.global.UserApiKeyAiPluginConfig gmsUserConfig =
          gmsConfig.getUserApiKeyConfig();
      if (gmsUserConfig != null) {
        UserApiKeyAiPluginConfig userConfig = new UserApiKeyAiPluginConfig();

        // Map auth injection settings with defaults matching PDL
        userConfig.setAuthLocation(
            gmsUserConfig.hasAuthLocation()
                ? AuthInjectionLocation.valueOf(gmsUserConfig.getAuthLocation().toString())
                : AuthInjectionLocation.HEADER);
        userConfig.setAuthHeaderName(
            gmsUserConfig.hasAuthHeaderName()
                ? gmsUserConfig.getAuthHeaderName()
                : "Authorization");
        if (gmsUserConfig.hasAuthScheme()) {
          userConfig.setAuthScheme(gmsUserConfig.getAuthScheme());
        }
        if (gmsUserConfig.hasAuthQueryParam()) {
          userConfig.setAuthQueryParam(gmsUserConfig.getAuthQueryParam());
        }

        result.setUserApiKeyConfig(userConfig);
      }
    }

    // Set partial Service for resolution by type resolver
    Service partialService = new Service();
    partialService.setUrn(gmsConfig.getServiceUrn().toString());
    result.setService(partialService);

    return result;
  }

  private MaintenanceWindowSettings mapMaintenanceWindowSettings(
      @Nonnull com.linkedin.settings.global.MaintenanceWindowSettings input) {
    MaintenanceWindowSettings result = new MaintenanceWindowSettings();
    result.setEnabled(input.isEnabled());
    if (input.hasMessage()) {
      result.setMessage(input.getMessage());
    }
    if (input.hasSeverity()) {
      result.setSeverity(mapMaintenanceSeverity(input.getSeverity()));
    }
    if (input.hasLinkUrl()) {
      result.setLinkUrl(input.getLinkUrl());
    }
    if (input.hasLinkText()) {
      result.setLinkText(input.getLinkText());
    }
    if (input.hasEnabledAt()) {
      result.setEnabledAt(input.getEnabledAt());
    }
    if (input.hasEnabledBy()) {
      result.setEnabledBy(input.getEnabledBy());
    }
    return result;
  }

  private MaintenanceSeverity mapMaintenanceSeverity(
      @Nonnull com.linkedin.settings.global.MaintenanceSeverity severity) {
    switch (severity) {
      case INFO:
        return MaintenanceSeverity.INFO;
      case WARNING:
        return MaintenanceSeverity.WARNING;
      case CRITICAL:
        return MaintenanceSeverity.CRITICAL;
      default:
        throw new IllegalArgumentException("Unknown MaintenanceSeverity: " + severity);
    }
  }
}
