package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class UpdateGlobalSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public UpdateGlobalSettingsResolver(final EntityClient entityClient, final SecretService secretService) {
    _entityClient = entityClient;
    _secretService = secretService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();


    return CompletableFuture.supplyAsync(() -> {
      if (SettingsMapper.canManageGlobalSettings(context)) {

        final UpdateGlobalSettingsInput input = bindArgument(environment.getArgument("input"), UpdateGlobalSettingsInput.class);

        // First, fetch the existing global settings.
        GlobalSettingsInfo globalSettings = SettingsMapper.getGlobalSettings(_entityClient, context.getAuthentication());

        // Next, patch the global settings.
        updateSettings(globalSettings, input);

        // Finally, write it back in a new aspect.
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(Constants.GLOBAL_SETTINGS_URN);
        proposal.setEntityType(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
        proposal.setAspectName(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(globalSettings));
        proposal.setChangeType(ChangeType.UPSERT);
          try {
            _entityClient.ingestProposal(proposal, context.getAuthentication());
            return true;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to update global settings! %s", input.toString()), e);
          }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private void updateSettings(final GlobalSettingsInfo existingSettings, final UpdateGlobalSettingsInput update) {
    if (update.getIntegrationSettings() != null) {
      updateGlobalIntegrationSettings(existingSettings.getIntegrations(), update.getIntegrationSettings());
    }
    if (update.getNotificationSettings() != null) {
      updateGlobalNotificationSettings(existingSettings.getNotifications(), update.getNotificationSettings());
    }
  }

  private void updateGlobalIntegrationSettings(
      final GlobalIntegrationSettings existingSettings,
      final UpdateGlobalIntegrationSettingsInput update) {
    if (update.getSlackSettings() != null) {
      updateSlackIntegrationSettings(
        existingSettings.hasSlackSettings() ? existingSettings.getSlackSettings() : new SlackIntegrationSettings(),
        update.getSlackSettings()
      );
    }
  }

  private void updateSlackIntegrationSettings(
      final SlackIntegrationSettings existingSettings,
      final UpdateSlackIntegrationSettingsInput update
  ) {
    existingSettings.setEnabled(update.getEnabled());
    if (update.getDefaultChannelName() != null) {
      existingSettings.setDefaultChannelName(update.getDefaultChannelName());
    }
    if (update.getBotToken() != null) {
      existingSettings.setEncryptedBotToken(_secretService.encrypt(update.getBotToken()));
    }
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
