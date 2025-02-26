package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateUserNotificationSettingsInput;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingMapMapper;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSettingMap;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UpdateUserNotificationSettingsResolver
    implements DataFetcher<CompletableFuture<NotificationSettings>> {
  private final SettingsService _settingsService;

  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment)
      throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final Authentication authentication = context.getAuthentication();
          final String userUrnString = context.getActorUrn();
          final UpdateUserNotificationSettingsInput input =
              bindArgument(
                  environment.getArgument("input"), UpdateUserNotificationSettingsInput.class);
          final NotificationSettingsInput notificationSettingsInput =
              input.getNotificationSettings();

          try {
            final Urn userUrn = UrnUtils.getUrn(userUrnString);

            CorpUserSettings corpUserSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);

            if (corpUserSettings == null) {
              // Copy to avoid changing a static reference at runtime.
              corpUserSettings = SettingsService.DEFAULT_CORP_USER_SETTINGS.copy();
            }

            final com.linkedin.event.notification.settings.NotificationSettings
                notificationSettings =
                    corpUserSettings.hasNotificationSettings()
                        ? corpUserSettings.getNotificationSettings()
                        : new com.linkedin.event.notification.settings.NotificationSettings()
                            .setSinkTypes(new NotificationSinkTypeArray());

            if (notificationSettingsInput.getSinkTypes() != null) {
              NotificationSinkTypeArray sinkTypes = new NotificationSinkTypeArray();
              input
                  .getNotificationSettings()
                  .getSinkTypes()
                  .forEach(type -> sinkTypes.add(NotificationSinkType.valueOf(type.toString())));
              notificationSettings.setSinkTypes(sinkTypes);
            }

            final SlackNotificationSettingsInput slackNotificationSettingsInput =
                notificationSettingsInput.getSlackSettings();
            if (slackNotificationSettingsInput != null) {
              final SlackNotificationSettings slackNotificationSettings =
                  _settingsService.createSlackNotificationSettings(
                      slackNotificationSettingsInput.getUserHandle(),
                      slackNotificationSettingsInput.getChannels());
              notificationSettings.setSlackSettings(slackNotificationSettings);
            }

            final EmailNotificationSettingsInput emailNotificationSettingsInput =
                notificationSettingsInput.getEmailSettings();
            if (emailNotificationSettingsInput != null) {
              final EmailNotificationSettings emailNotificationSettings =
                  _settingsService.createEmailNotificationSettings(
                      emailNotificationSettingsInput.getEmail());
              notificationSettings.setEmailSettings(emailNotificationSettings);
            }

            if (input.getNotificationSettings().getSettings() != null) {
              NotificationSettingMap newSettings =
                  NotificationSettingMapMapper.mapNotificationSettingInputList(
                      input.getNotificationSettings().getSettings());
              if (notificationSettings.hasSettings()) {
                // Simply overwrite what's already there.
                notificationSettings.getSettings().putAll(newSettings);
              } else {
                notificationSettings.setSettings(newSettings);
              }
            }

            corpUserSettings.setNotificationSettings(notificationSettings);
            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, corpUserSettings);

            return NotificationSettingsMapper.map(context, notificationSettings);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to update notification settings for user %s", userUrnString, e));
          }
        });
  }
}
