package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.EmailIntegrationSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OidcSettings;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.linkedin.settings.global.SsoSettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class UpdateGlobalSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public UpdateGlobalSettingsResolver(
      final EntityClient entityClient, final SecretService secretService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _secretService = Objects.requireNonNull(secretService, "secretService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (SettingsMapper.canManageGlobalSettings(context)) {

            final UpdateGlobalSettingsInput input =
                bindArgument(environment.getArgument("input"), UpdateGlobalSettingsInput.class);

            // First, fetch the existing global settings.
            GlobalSettingsInfo globalSettings =
                SettingsMapper.getGlobalSettings(context.getOperationContext(), _entityClient);

            // Next, patch the global settings.
            updateSettings(globalSettings, input);

            // Finally, write it back in a new aspect.
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, globalSettings);
            try {
              _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update global settings! %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private void updateSettings(
      final GlobalSettingsInfo existingSettings, final UpdateGlobalSettingsInput update) {
    if (update.getIntegrationSettings() != null) {
      updateGlobalIntegrationSettings(
          existingSettings.getIntegrations(), update.getIntegrationSettings());
    }
    if (update.getNotificationSettings() != null) {
      updateGlobalNotificationSettings(
          existingSettings.getNotifications(), update.getNotificationSettings());
    }
    if (update.getSsoSettings() != null) {
      SsoSettings existingSsoSettings =
          existingSettings.hasSso() ? existingSettings.getSso() : new SsoSettings();
      updateSsoSettings(existingSsoSettings, update.getSsoSettings());
      existingSettings.setSso(existingSsoSettings);
    }
  }

  private void updateGlobalIntegrationSettings(
      final GlobalIntegrationSettings existingSettings,
      final UpdateGlobalIntegrationSettingsInput update) {
    if (update.getSlackSettings() != null) {
      SlackIntegrationSettings existingSlackSettings =
          existingSettings.hasSlackSettings()
              ? existingSettings.getSlackSettings()
              : new SlackIntegrationSettings();
      updateSlackIntegrationSettings(existingSlackSettings, update.getSlackSettings());
      existingSettings.setSlackSettings(existingSlackSettings);
    }
    if (update.getEmailSettings() != null) {
      EmailIntegrationSettings existingEmailSettings =
          existingSettings.hasEmailSettings()
              ? existingSettings.getEmailSettings()
              : new EmailIntegrationSettings();
      updateEmailIntegrationSettings(existingEmailSettings, update.getEmailSettings());
      existingSettings.setEmailSettings(existingEmailSettings);
    }
  }

  private void updateSlackIntegrationSettings(
      final SlackIntegrationSettings existingSettings,
      final UpdateSlackIntegrationSettingsInput update) {
    existingSettings.setEnabled(true);
    if (update.getDefaultChannelName() != null) {
      existingSettings.setDefaultChannelName(update.getDefaultChannelName());
    }
    if (update.getBotToken() != null) {
      existingSettings.setEncryptedBotToken(_secretService.encrypt(update.getBotToken()));
    }
  }

  private void updateEmailIntegrationSettings(
      final EmailIntegrationSettings existingSettings,
      final UpdateEmailIntegrationSettingsInput update) {
    existingSettings.setDefaultEmail(update.getDefaultEmail(), SetMode.IGNORE_NULL);
  }

  private void updateGlobalNotificationSettings(
      final GlobalNotificationSettings existingSettings,
      final UpdateGlobalNotificationSettingsInput update) {
    if (update.getSettings() != null) {
      NotificationSettingMap newSettings = mapSettings(update.getSettings());
      if (existingSettings.hasSettings()) {
        // Simply overwrite what's already there.
        existingSettings.getSettings().putAll(newSettings);
      } else {
        existingSettings.setSettings(newSettings);
      }
    }
  }

  private void updateSsoSettings(
      final SsoSettings ssoSettings, final UpdateSsoSettingsInput update) {
    ssoSettings.setBaseUrl(update.getBaseUrl());

    if (update.getOidcSettings() != null) {
      OidcSettings oidcSettings =
          ssoSettings.hasOidcSettings() ? ssoSettings.getOidcSettings() : new OidcSettings();
      updateOidcSettings(oidcSettings, update.getOidcSettings());
      ssoSettings.setOidcSettings(oidcSettings);
    }
  }

  private void updateOidcSettings(
      final OidcSettings oidcSettings, final UpdateOidcSettingsInput update) {
    oidcSettings.setEnabled(update.getEnabled());
    oidcSettings.setClientId(update.getClientId());
    oidcSettings.setClientSecret(_secretService.encrypt(update.getClientSecret()));
    oidcSettings.setDiscoveryUri(update.getDiscoveryUri());
    if (update.getUserNameClaim() != null) {
      oidcSettings.setUserNameClaim(update.getUserNameClaim());
    }
    if (update.getUserNameClaimRegex() != null) {
      oidcSettings.setUserNameClaimRegex(update.getUserNameClaimRegex());
    }
    if (update.getScope() != null) {
      oidcSettings.setScope(update.getScope());
    }
    if (update.getClientAuthenticationMethod() != null) {
      oidcSettings.setClientAuthenticationMethod(update.getClientAuthenticationMethod());
    }
    if (update.getJitProvisioningEnabled() != null) {
      oidcSettings.setJitProvisioningEnabled(update.getJitProvisioningEnabled());
    }
    if (update.getPreProvisioningRequired() != null) {
      oidcSettings.setPreProvisioningRequired(update.getPreProvisioningRequired());
    }
    if (update.getExtractGroupsEnabled() != null) {
      oidcSettings.setExtractGroupsEnabled(update.getExtractGroupsEnabled());
    }
    if (update.getGroupsClaim() != null) {
      oidcSettings.setGroupsClaim(update.getGroupsClaim());
    }
    if (update.getResponseType() != null) {
      oidcSettings.setResponseType(update.getResponseType());
    }
    if (update.getResponseMode() != null) {
      oidcSettings.setResponseMode(update.getResponseMode());
    }
    if (update.getUseNonce() != null) {
      oidcSettings.setUseNonce(update.getUseNonce());
    }
    if (update.getReadTimeout() != null) {
      oidcSettings.setReadTimeout(update.getReadTimeout());
    }
    if (update.getExtractJwtAccessTokenClaims() != null) {
      oidcSettings.setExtractJwtAccessTokenClaims(update.getExtractJwtAccessTokenClaims());
    }
    if (update.getPreferredJwsAlgorithm() != null) {
      oidcSettings.setPreferredJwsAlgorithm2(update.getPreferredJwsAlgorithm());
    }
    oidcSettings.removePreferredJwsAlgorithm();
  }

  private NotificationSettingMap mapSettings(List<NotificationSettingInput> updatedSettings) {
    NotificationSettingMap map = new NotificationSettingMap();
    for (NotificationSettingInput input : updatedSettings) {
      NotificationSetting notificationSetting = new NotificationSetting();
      notificationSetting.setValue(NotificationSettingValue.valueOf(input.getValue().toString()));
      if (input.getParams() != null) {
        notificationSetting.setParams(mapParams(input.getParams()));
      }
      map.put(input.getType().toString(), notificationSetting);
    }
    return map;
  }

  private StringMap mapParams(List<StringMapEntryInput> input) {
    final StringMap result = new StringMap();
    for (StringMapEntryInput entry : input) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
